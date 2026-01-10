from __future__ import annotations

import argparse
import io
import json
import re
import sys
import hashlib
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlencode

import requests


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_PUBLISH_PATH = "/extensions/admin/extensions/publish"

SEED_URLS = [
    "https://openvsx.eclipsecontent.org/redhat/vscode-yaml/1.20.2026010808/redhat.vscode-yaml-1.20.2026010808.vsix",
    "https://openvsx.eclipsecontent.org/KylinIdeTeam/cppdebug/linux-x64/0.1.0/KylinIdeTeam.cppdebug-0.1.0@linux-x64.vsix",
]

ALLOWED_TP = {
    "win32-x64",
    "win32-ia32",
    "win32-arm64",
    "linux-x64",
    "linux-arm64",
    "linux-armhf",
    "alpine-x64",
    "alpine-arm64",
    "darwin-x64",
    "darwin-arm64",
    "web",
    "universal",
}


@dataclass(frozen=True)
class VsixMeta:
    publisher: str
    name: str
    version: str
    target_platform: str  # "universal" if unknown
    sha256: str
    filename: str
    source_url: str


@dataclass
class Check:
    name: str
    ok: bool
    status: int
    detail: str


def eprint(*a: Any) -> None:
    print(*a, file=sys.stderr)


def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def safe_segment(s: str) -> str:
    s = (s or "").strip()
    if not s or s in (".", ".."):
        raise ValueError("empty segment")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", s):
        raise ValueError(f"bad segment: {s}")
    return s


def req(
    sess: requests.Session,
    method: str,
    url: str,
    *,
    expected: Tuple[int, ...],
    headers: Optional[Dict[str, str]] = None,
    data: Optional[bytes] = None,
    stream: bool = False,
    timeout: int = 60,
) -> requests.Response:
    r = sess.request(method, url, headers=headers, data=data, stream=stream, timeout=timeout)
    if r.status_code not in expected:
        body = ""
        try:
            body = r.text[:800]
        except Exception:
            body = "<non-text>"
        raise RuntimeError(f"{method} {url} -> {r.status_code}, expected {expected}. Body: {body}")
    return r


def read_json_from_zip(zf: zipfile.ZipFile, path: str) -> Optional[Dict[str, Any]]:
    try:
        raw = zf.read(path)
    except KeyError:
        return None
    try:
        return json.loads(raw.decode("utf-8", errors="strict"))
    except Exception:
        return None


def read_text_from_zip(zf: zipfile.ZipFile, path: str) -> Optional[str]:
    try:
        raw = zf.read(path)
    except KeyError:
        return None
    try:
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return None


def parse_target_platform_from_manifest(manifest_text: str) -> Optional[str]:
    m = re.search(
        r'Id\s*=\s*"[^"]*TargetPlatform[^"]*"\s+Value\s*=\s*"([^"]+)"',
        manifest_text,
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    tp = (m.group(1) or "").strip().lower()
    return tp or None


def infer_target_platform_from_filename_or_url(source_url: str, filename: str) -> Optional[str]:
    m = re.search(r"@([A-Za-z0-9._-]+)\.vsix$", filename, flags=re.IGNORECASE)
    if m:
        return m.group(1).lower()

    path = urlparse(source_url).path.lower()
    for tp in ALLOWED_TP:
        if f"/{tp}/" in path:
            return tp
    return None


def extract_vsix_meta(vsix_bytes: bytes, source_url: str) -> VsixMeta:
    if len(vsix_bytes) < 4 or vsix_bytes[:2] != b"PK":
        raise ValueError("not a zip/vsix (missing PK header)")

    with zipfile.ZipFile(io.BytesIO(vsix_bytes)) as zf:
        pkg = None
        for cand in ("extension/package.json", "package.json"):
            pkg = read_json_from_zip(zf, cand)
            if pkg is not None:
                break
        if not pkg:
            raise ValueError("VSIX missing package.json")

        publisher = safe_segment(str(pkg.get("publisher", "")).strip())
        name = safe_segment(str(pkg.get("name", "")).strip())
        version = str(pkg.get("version", "")).strip()
        if not version:
            raise ValueError("VSIX missing version field in package.json")

        tp: Optional[str] = None

        manifest_text = None
        for cand in ("extension.vsixmanifest", "extension/extension.vsixmanifest"):
            manifest_text = read_text_from_zip(zf, cand)
            if manifest_text:
                break

        if manifest_text:
            tp = parse_target_platform_from_manifest(manifest_text)

        filename_guess = f"{publisher}.{name}-{version}.vsix"
        tp2 = infer_target_platform_from_filename_or_url(
            source_url,
            Path(urlparse(source_url).path).name or filename_guess,
        )

        if tp and tp not in ALLOWED_TP:
            tp = None
        if tp is None and tp2 and tp2 in ALLOWED_TP:
            tp = tp2
        if tp is None:
            tp = "universal"

        url_name = Path(urlparse(source_url).path).name
        filename = url_name if url_name else filename_guess

        return VsixMeta(
            publisher=publisher,
            name=name,
            version=version,
            target_platform=tp,
            sha256=sha256_bytes(vsix_bytes),
            filename=filename,
            source_url=source_url,
        )


def download_vsix(sess: requests.Session, url: str, workdir: Path, force: bool) -> Tuple[Path, VsixMeta, bytes]:
    workdir.mkdir(parents=True, exist_ok=True)
    fn = Path(urlparse(url).path).name or "download.vsix"
    target = workdir / fn

    if target.exists() and not force:
        b = target.read_bytes()
        meta = extract_vsix_meta(b, url)
        return target, meta, b

    r = req(sess, "GET", url, expected=(200,), stream=True)
    b = r.content
    target.write_bytes(b)
    meta = extract_vsix_meta(b, url)
    return target, meta, b


def publish_vsix(sess: requests.Session, base_url: str, publish_path: str, vsix_bytes: bytes) -> Dict[str, Any]:
    url = base_url.rstrip("/") + publish_path
    r = req(
        sess,
        "POST",
        url,
        expected=(201,),
        headers={"Content-Type": "application/octet-stream"},
        data=vsix_bytes,
    )
    try:
        return r.json()
    except Exception as exc:
        raise RuntimeError(f"Publish response is not JSON: {exc}. Body: {r.text[:800]}") from exc


def get_json(sess: requests.Session, url: str, expected: Tuple[int, ...] = (200,)) -> Dict[str, Any]:
    r = req(sess, "GET", url, expected=expected)
    try:
        return r.json()
    except Exception as exc:
        raise RuntimeError(f"GET {url} not JSON: {exc}. Body: {r.text[:800]}") from exc


def download_bytes(sess: requests.Session, url: str, expected: Tuple[int, ...] = (200,)) -> bytes:
    r = req(sess, "GET", url, expected=expected, stream=True)
    return r.content


def _api_base(base_url: str) -> str:
    return base_url.rstrip("/") + "/api"


def _api_query_url(api_base: str, params: Dict[str, Any], *, v1: bool = False) -> str:
    path = "/v1/-/query" if v1 else "/-/query"
    qs = urlencode({k: v for k, v in params.items() if v is not None and v != ""})
    return f"{api_base}{path}?{qs}" if qs else f"{api_base}{path}"


def check_one(
    sess: requests.Session,
    base_url: str,
    meta: VsixMeta,
    publish_resp: Dict[str, Any],
) -> List[Check]:
    checks: List[Check] = []
    api = _api_base(base_url)

    def ok(name: str, status: int, detail: str) -> None:
        checks.append(Check(name=name, ok=True, status=status, detail=detail))

    def bad(name: str, status: int, detail: str) -> None:
        checks.append(Check(name=name, ok=False, status=status, detail=detail))

    def skip(name: str, detail: str) -> None:
        # "skip" is reported as OK with status 204 to avoid failing overall when not applicable
        checks.append(Check(name=name, ok=True, status=204, detail=detail))

    ns = meta.publisher
    ext = meta.name
    ver = meta.version
    tp = (meta.target_platform or "universal").lower()
    if tp not in ALLOWED_TP:
        tp = "universal"

    stored_filename = f"{ns}.{ext}-{ver}.vsix"

    # 0) Query endpoints
    for v1 in (False, True):
        qurl = _api_query_url(
            api,
            {
                "extensionId": f"{ns}.{ext}",
                "includeAllVersions": "false",
                "targetPlatform": tp,
                "size": 5,
                "offset": 0,
            },
            v1=v1,
        )
        try:
            j = get_json(sess, qurl)
            if "extensions" not in j or not isinstance(j.get("extensions"), list):
                raise RuntimeError("query response missing/invalid 'extensions'")
            ok("GET query" + (" (v1)" if v1 else ""), 200, qurl)
        except Exception as exc:
            bad("GET query" + (" (v1)" if v1 else ""), 0, f"{qurl} :: {exc}")

    # 1) Namespace
    url = f"{api}/{ns}"
    try:
        j = get_json(sess, url)
        if j.get("name") != ns:
            raise RuntimeError(f"expected name={ns}, got {j.get('name')}")
        ok("GET namespace", 200, url)
    except Exception as exc:
        bad("GET namespace", 0, f"{url} :: {exc}")

    # 2) Extension version
    url = f"{api}/{ns}/{ext}/{ver}"
    try:
        j = get_json(sess, url)
        if j.get("namespace") != ns or j.get("name") != ext:
            raise RuntimeError("namespace/name mismatch")
        if j.get("version") != ver:
            raise RuntimeError(f"version mismatch: {j.get('version')} != {ver}")
        dl = j.get("files", {}).get("download")
        if not isinstance(dl, str) or not dl:
            raise RuntimeError("missing files.download")
        ok("GET extension version", 200, url)
    except Exception as exc:
        bad("GET extension version", 0, f"{url} :: {exc}")

    # 3) Extension platform+version
    url = f"{api}/{ns}/{ext}/{tp}/{ver}"
    try:
        j = get_json(sess, url)
        if j.get("namespace") != ns or j.get("name") != ext:
            raise RuntimeError("namespace/name mismatch")
        if j.get("version") != ver:
            raise RuntimeError("version mismatch")
        ok("GET extension platform+version", 200, url)
    except Exception as exc:
        bad("GET extension platform+version", 0, f"{url} :: {exc}")

    # 4) Download from publish response (may be /extensions/api/... or /api/...)
    dl_url = None
    try:
        dl_url = publish_resp.get("files", {}).get("download")
    except Exception:
        dl_url = None

    if isinstance(dl_url, str) and dl_url:
        try:
            b = download_bytes(sess, dl_url)
            if len(b) < 4 or b[:2] != b"PK":
                raise RuntimeError("download not zip/vsix")
            ok("GET files.download (from publish)", 200, dl_url)
        except Exception as exc:
            bad("GET files.download (from publish)", 0, f"{dl_url} :: {exc}")
    else:
        bad("GET files.download (from publish)", 0, "publish response missing files.download")

    # 5) Direct file endpoints
    # IMPORTANT: /api/{ns}/{ext}/{ver}/file/... maps ONLY to ".../{ver}/universal/..."
    # For non-universal artifacts this is expected to be 404. Do not fail.
    url_univ = f"{api}/{ns}/{ext}/{ver}/file/{stored_filename}"
    if tp != "universal":
        try:
            _ = download_bytes(sess, url_univ, expected=(200, 404))
            # If it's 404, it's fine (platform-only extension)
            r = sess.get(url_univ, stream=True, timeout=60)
            if r.status_code == 200:
                b = r.content
                if len(b) < 4 or b[:2] != b"PK":
                    raise RuntimeError("not vsix")
                ok("GET file (universal)", 200, url_univ)
            else:
                skip("GET file (universal)", f"{url_univ} :: not applicable for tp={tp} (expected 404)")
        except Exception as exc:
            bad("GET file (universal)", 0, f"{url_univ} :: {exc}")
    else:
        try:
            b = download_bytes(sess, url_univ)
            if len(b) < 4 or b[:2] != b"PK":
                raise RuntimeError("not vsix")
            ok("GET file (universal)", 200, url_univ)
        except Exception as exc:
            bad("GET file (universal)", 0, f"{url_univ} :: {exc}")

    # Platform path should exist when tp != universal
    url_plat = f"{api}/{ns}/{ext}/{tp}/{ver}/file/{stored_filename}"
    try:
        b = download_bytes(sess, url_plat)
        if len(b) < 4 or b[:2] != b"PK":
            raise RuntimeError("not vsix")
        ok("GET file (platform)", 200, url_plat)
    except Exception as exc:
        # If tp==universal, platform route is still valid in your server, so we keep it as a real check too.
        bad("GET file (platform)", 0, f"{url_plat} :: {exc}")

    # 6) integrity
    integrity_url: Optional[str] = None
    if isinstance(dl_url, str) and dl_url:
        integrity_url = dl_url
    else:
        integrity_url = url_plat if tp != "universal" else url_univ

    if integrity_url:
        try:
            b = download_bytes(sess, integrity_url)
            got = sha256_bytes(b)
            if got != meta.sha256:
                raise RuntimeError(f"sha256 mismatch: got {got} expected {meta.sha256}")
            ok("SHA256 integrity (roundtrip)", 200, got)
        except Exception as exc:
            bad("SHA256 integrity (roundtrip)", 0, str(exc))

    return checks


def print_report(meta: VsixMeta, checks: List[Check]) -> bool:
    print(f"\n=== {meta.publisher}.{meta.name}@{meta.version} tp={meta.target_platform} sha256={meta.sha256[:12]}… ===")
    ok_all = True
    for c in checks:
        flag = "OK" if c.ok else "FAIL"
        print(f"[{flag}] {c.name} (status={c.status}) {c.detail}")
        if not c.ok:
            ok_all = False
    return ok_all


def main() -> int:
    ap = argparse.ArgumentParser(description="Autonomous VSIX migrator + OpenVSX-compatible API checker (no auth).")
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"Target service base URL (default {DEFAULT_BASE_URL})")
    ap.add_argument("--publish-path", default=DEFAULT_PUBLISH_PATH, help=f"Publish path (default {DEFAULT_PUBLISH_PATH})")
    ap.add_argument("--workdir", default=".vsix_work", help="Working directory for downloads/cache")
    ap.add_argument("--force", action="store_true", help="Re-download even if file exists in workdir")
    ap.add_argument("--urls", nargs="*", default=SEED_URLS, help="VSIX URLs to migrate/check")
    args = ap.parse_args()

    base_url = args.base_url.rstrip("/")
    publish_path = args.publish_path
    workdir = Path(args.workdir)

    sess = requests.Session()

    overall_ok = True
    for url in args.urls:
        try:
            file_path, meta, vsix_bytes = download_vsix(sess, url, workdir, args.force)
            print(f"Downloaded: {url}")
            print(f"Saved as:  {file_path}")
            print(f"Parsed:    {meta.publisher}.{meta.name}@{meta.version} tp={meta.target_platform} sha256={meta.sha256[:12]}…")
        except Exception as exc:
            eprint(f"ERROR downloading/parsing {url}: {exc}")
            overall_ok = False
            continue

        try:
            pub = publish_vsix(sess, base_url, publish_path, vsix_bytes)
            for k in ("namespace", "name", "version", "files"):
                if k not in pub:
                    raise RuntimeError(f"Publish response missing '{k}': {pub}")
            print(f"Published to {base_url}{publish_path}: {pub.get('namespace')}.{pub.get('name')}@{pub.get('version')}")
        except Exception as exc:
            eprint(f"ERROR publishing {meta.filename}: {exc}")
            overall_ok = False
            continue

        checks = check_one(sess, base_url, meta, pub)
        ok = print_report(meta, checks)
        overall_ok = overall_ok and ok

    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
