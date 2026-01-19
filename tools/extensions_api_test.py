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
from urllib.parse import urlparse

import requests


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_PUBLISH_PATH = "/api/user/publish"

SEED_URLS = [
    "https://openvsx.eclipsecontent.org/redhat/vscode-yaml/1.20.2026010808/redhat.vscode-yaml-1.20.2026010808.vsix",
    "https://openvsx.eclipsecontent.org/KylinIdeTeam/cppdebug/linux-x64/0.1.0/KylinIdeTeam.cppdebug-0.1.0@linux-x64.vsix",
    "https://openvsx.eclipsecontent.org/KylinIdeTeam/cppdebug/win32-x64/0.1.0/KylinIdeTeam.cppdebug-0.1.0@win32-x64.vsix",
    "https://openvsx.eclipsecontent.org/KylinIdeTeam/cppdebug/darwin-x64/0.0.7/KylinIdeTeam.cppdebug-0.0.7@darwin-x64.vsix",
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
    target_platform: str
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


def normalize_tp(tp: str) -> str:
    v = (tp or "").strip().lower()
    if not v:
        return "universal"
    return v if v in ALLOWED_TP else "universal"


def req(
    sess: requests.Session,
    method: str,
    url: str,
    *,
    expected: Tuple[int, ...],
    headers: Optional[Dict[str, str]] = None,
    data: Any = None,
    json_body: Optional[Dict[str, Any]] = None,
    files: Any = None,
    stream: bool = False,
    timeout: int = 60,
    allow_redirects: bool = True,
) -> requests.Response:
    r = sess.request(
        method,
        url,
        headers=headers,
        data=data,
        json=json_body,
        files=files,
        stream=stream,
        timeout=timeout,
        allow_redirects=allow_redirects,
    )
    if r.status_code not in expected:
        body = ""
        try:
            body = r.text[:1200]
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
    m_identity = re.search(
        r"<\s*Identity\b[^>]*\bTargetPlatform\s*=\s*\"([^\"]+)\"",
        manifest_text,
        flags=re.IGNORECASE,
    )
    if m_identity:
        tp = (m_identity.group(1) or "").strip().lower()
        return tp or None

    m_prop = re.search(
        r'Id\s*=\s*"[^"]*TargetPlatform[^"]*"\s+Value\s*=\s*"([^"]+)"',
        manifest_text,
        flags=re.IGNORECASE,
    )
    if not m_prop:
        return None
    tp = (m_prop.group(1) or "").strip().lower()
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


def publish_openvsx_like(
    sess: requests.Session,
    base_url: str,
    publish_path: str,
    vsix_bytes: bytes,
    *,
    tp: str,
    filename: str,
) -> Dict[str, Any]:
    url = base_url.rstrip("/") + publish_path
    tp_n = normalize_tp(tp)

    # OpenVSX publish: multipart/form-data, "file" + form "targetPlatform"
    files = {"file": (filename or "extension.vsix", vsix_bytes, "application/octet-stream")}
    data = {}
    if tp_n and tp_n != "universal":
        data["targetPlatform"] = tp_n

    r = req(sess, "POST", url, expected=(200, 201), files=files, data=data, headers={"Accept": "application/json"})
    j = r.json()
    return j if isinstance(j, dict) else {"raw": j}


def download_bytes(sess: requests.Session, url: str, expected: Tuple[int, ...] = (200,)) -> bytes:
    r = req(sess, "GET", url, expected=expected, stream=True)
    return r.content


def post_json(sess: requests.Session, url: str, body: Dict[str, Any], expected: Tuple[int, ...] = (200,)) -> Dict[str, Any]:
    r = req(
        sess,
        "POST",
        url,
        expected=expected,
        headers={"Content-Type": "application/json", "Accept": "application/json;api-version=3.0-preview.1"},
        json_body=body,
    )
    return r.json()


def get_json(sess: requests.Session, url: str, expected: Tuple[int, ...] = (200,)) -> Dict[str, Any]:
    r = req(sess, "GET", url, expected=expected)
    return r.json()


def vscode_extensionquery_body(ext_id: str, tp: str) -> Dict[str, Any]:
    # Ваша реализация FILTER_TARGET (8) ожидает именно targetPlatform (win32-x64 и т.п.)
    return {
        "filters": [
            {
                "criteria": [
                    {"filterType": 8, "value": tp},      # targetPlatform
                    {"filterType": 4, "value": ext_id},  # extensionId
                ],
                "pageNumber": 1,
                "pageSize": 20,
                "sortBy": 0,
                "sortOrder": 0,
            }
        ],
        "flags": 0x1 | 0x2 | 0x80 | 0x10 | 0x100,
    }


def pick_vsix_source_from_extensionquery(
    resp_json: Dict[str, Any],
    ext_id: str,
    *,
    prefer_version: Optional[str] = None,
) -> Tuple[str, str]:
    results = resp_json.get("results")
    if not isinstance(results, list) or not results:
        raise RuntimeError("extensionquery: missing results")

    exts = results[0].get("extensions")
    if not isinstance(exts, list):
        raise RuntimeError("extensionquery: missing extensions list")

    want = (ext_id or "").strip().lower()
    target = None
    for x in exts:
        if not isinstance(x, dict):
            continue
        got = str(x.get("extensionId") or "").strip().lower()
        if got == want:
            target = x
            break

    if not target:
        got_ids = []
        for x in exts:
            if isinstance(x, dict) and x.get("extensionId"):
                got_ids.append(str(x.get("extensionId")))
        raise RuntimeError(f"extensionquery: extensionId not found: {ext_id} (got: {got_ids})")

    versions = target.get("versions")
    if not isinstance(versions, list) or not versions:
        raise RuntimeError("extensionquery: missing versions")

    def extract_first_source(v: Dict[str, Any]) -> Optional[str]:
        files = v.get("files")
        if not isinstance(files, list):
            return None
        for f in files:
            if isinstance(f, dict) and isinstance(f.get("source"), str) and f["source"]:
                return f["source"]
        return None

    # 1) Prefer an exact version match if requested
    if prefer_version:
        pv = str(prefer_version).strip()
        for v in versions:
            if not isinstance(v, dict):
                continue
            ver = str(v.get("version") or "").strip()
            if ver == pv:
                src = extract_first_source(v)
                if src:
                    return ver, src

    # 2) Fallback: first available source (usually latest)
    for v in versions:
        if not isinstance(v, dict):
            continue
        ver = str(v.get("version") or "").strip()
        src = extract_first_source(v)
        if src:
            return ver, src

    raise RuntimeError("extensionquery: no versions[].files[].source found")

def print_report(title: str, checks: List[Check]) -> bool:
    print(f"\n--- {title} ---")
    ok_all = True
    for c in checks:
        flag = "OK" if c.ok else "FAIL"
        print(f"[{flag}] {c.name} (status={c.status}) {c.detail}")
        if not c.ok:
            ok_all = False
    return ok_all


def check_marketplace_flow(sess: requests.Session, base_url: str, meta: VsixMeta) -> List[Check]:
    checks: List[Check] = []
    vscode = base_url.rstrip("/") + "/vscode"

    def ok(name: str, status: int, detail: str) -> None:
        checks.append(Check(name=name, ok=True, status=status, detail=detail))

    def bad(name: str, status: int, detail: str) -> None:
        checks.append(Check(name=name, ok=False, status=status, detail=detail))

    tp = normalize_tp(meta.target_platform or "universal")
    ext_id = f"{meta.publisher}.{meta.name}"

    # 1) SEARCH
    qurl = f"{vscode}/gallery/extensionquery"
    try:
        body = vscode_extensionquery_body(ext_id, tp)
        j = post_json(sess, qurl, body, expected=(200,))
        ok("POST /vscode/gallery/extensionquery", 200, qurl)
    except Exception as exc:
        bad("POST /vscode/gallery/extensionquery", 0, f"{qurl} :: {exc}")
        return checks

    # 2) PICK DOWNLOAD URL
    try:
        ver_found, src = pick_vsix_source_from_extensionquery(j, ext_id, prefer_version=meta.version)
        ok("extensionquery: picked files.source", 200, src)
        if ver_found and ver_found != meta.version:
            ok("extensionquery: version note", 200, f"found={ver_found}, expected={meta.version}")
    except Exception as exc:
        bad("extensionquery: pick files.source", 0, str(exc))
        return checks

    # 3) DOWNLOAD VIA MARKETPLACE URL
    try:
        b = download_bytes(sess, src, expected=(200,))
        if len(b) < 4 or b[:2] != b"PK":
            raise RuntimeError("downloaded bytes are not a VSIX/zip")
        got = sha256_bytes(b)
        if got != meta.sha256:
            raise RuntimeError(f"sha256 mismatch: got {got} expected {meta.sha256}")
        ok("Download VSIX via files.source + SHA256", 200, src)
    except Exception as exc:
        bad("Download VSIX via files.source + SHA256", 0, f"{src} :: {exc}")

    # 4) LATEST (optional but should work in вашей реализации)
    latest_url = f"{vscode}/gallery/{meta.publisher}/{meta.name}/latest?targetPlatform={tp}"
    try:
        j2 = get_json(sess, latest_url, expected=(200,))
        got_id = str(j2.get("extensionId") or "").strip().lower()
        want_id = str(ext_id or "").strip().lower()
        if got_id != want_id:
            raise RuntimeError(f"extensionId mismatch: got={j2.get('extensionId')} expected={ext_id}")

        ok("GET /vscode/gallery/{ns}/{ext}/latest", 200, latest_url)
    except Exception as exc:
        bad("GET /vscode/gallery/{ns}/{ext}/latest", 0, f"{latest_url} :: {exc}")

    # 5) UNPKG root listing (optional; если unpacked есть — будет 200, если нет — может быть 200 (zip listing) тоже)
    unpkg_root = f"{vscode}/unpkg/{meta.publisher}/{meta.name}/{meta.version}/?targetPlatform={tp}"
    try:
        j3 = get_json(sess, unpkg_root, expected=(200,))
        if not isinstance(j3, list):
            raise RuntimeError("expected list response")
        ok("GET /vscode/unpkg/.../", 200, unpkg_root)
    except Exception as exc:
        bad("GET /vscode/unpkg/.../", 0, f"{unpkg_root} :: {exc}")

    return checks


def main() -> int:
    ap = argparse.ArgumentParser(description="OpenVSX-like publish + VS Code Marketplace client flow checker.")
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

        # publish (OpenVSX-style)
        try:
            pub = publish_openvsx_like(
                sess,
                base_url,
                publish_path,
                vsix_bytes,
                tp=meta.target_platform,
                filename=meta.filename,
            )
            print(f"Published to {base_url}{publish_path}: {meta.publisher}.{meta.name}@{meta.version} tp={meta.target_platform}")
        except Exception as exc:
            eprint(f"ERROR publishing {meta.filename}: {exc}")
            overall_ok = False
            continue

        # marketplace client flow
        title = f"{meta.publisher}.{meta.name}@{meta.version} tp={meta.target_platform} sha256={meta.sha256[:12]}…"
        checks = check_marketplace_flow(sess, base_url, meta)
        ok_all = print_report(title + " :: Marketplace flow", checks)
        overall_ok = overall_ok and ok_all

    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())