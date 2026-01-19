from __future__ import annotations

import io
import json
import logging
import mimetypes
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, Response, abort, jsonify, request, send_file, g

from services.extensions_registry import ALLOWED_PLATFORMS, REGISTRY
from services.releases import RELEASES_ROOT, UNIVERSAL_PLATFORM, get_latest_version_from_symlinks


bp_marketplace = Blueprint("marketplace_api", __name__)

ASSET_ICON = "Microsoft.VisualStudio.Services.Icons.Default"
ASSET_DETAILS = "Microsoft.VisualStudio.Services.Content.Details"
ASSET_CHANGELOG = "Microsoft.VisualStudio.Services.Content.Changelog"
ASSET_MANIFEST = "Microsoft.VisualStudio.Code.Manifest"
ASSET_VSIX = "Microsoft.VisualStudio.Services.VSIXPackage"
ASSET_LICENSE = "Microsoft.VisualStudio.Services.Content.License"
ASSET_WEB_RESOURCES = "Microsoft.VisualStudio.Code.WebResources"
ASSET_VSIXMANIFEST = "Microsoft.VisualStudio.Services.VsixManifest"

FLAG_INCLUDE_VERSIONS = 0x1
FLAG_INCLUDE_FILES = 0x2
FLAG_INCLUDE_CATEGORY_AND_TAGS = 0x4
FLAG_INCLUDE_VERSION_PROPERTIES = 0x10
FLAG_INCLUDE_ASSET_URI = 0x80
FLAG_INCLUDE_STATISTICS = 0x100
FLAG_INCLUDE_LATEST_VERSION_ONLY = 0x200

FILTER_TAG = 1
FILTER_EXTENSION_ID = 4
FILTER_EXTENSION_NAME = 7
FILTER_TARGET = 8
FILTER_SEARCH_TEXT = 10

_ALLOWED_ORIGINS = {"vscode-file://vscode-app"}
_ALLOWED_METHODS = "GET, POST, OPTIONS"

_log = logging.getLogger("marketplace")


def _releases_ext_root() -> Path:
    root = Path(RELEASES_ROOT).expanduser()

    cand_direct = root / "extensions"
    if cand_direct.is_dir():
        return cand_direct

    cand_nested = root / "releases" / "extensions"
    if cand_nested.is_dir():
        return cand_nested

    if root.name == "releases":
        cand_parent_direct = root.parent / "releases" / "extensions"
        if cand_parent_direct.is_dir():
            return cand_parent_direct

    return cand_direct


def _ext_dir(ns: str, ext: str, ver: str, tp: str) -> Path:
    return _releases_ext_root() / ns / ext / ver / tp


def _vsix_path(ns: str, ext: str, ver: str, tp: str) -> Path:
    return _ext_dir(ns, ext, ver, tp) / "extension.vsix"


def _unpacked_dir(ns: str, ext: str, ver: str, tp: str) -> Path:
    return _ext_dir(ns, ext, ver, tp) / "unpacked"


def _base_url() -> str:
    return request.host_url.rstrip("/")


def _get_cors_origin() -> str:
    origin = (request.headers.get("Origin") or "").strip()
    return origin if origin in _ALLOWED_ORIGINS else ""


def _apply_cors_headers(resp: Response) -> Response:
    origin = _get_cors_origin()
    if not origin:
        return resp

    resp.headers["Access-Control-Allow-Origin"] = origin
    resp.headers["Vary"] = "Origin"
    resp.headers["Access-Control-Allow-Methods"] = _ALLOWED_METHODS
    resp.headers["Access-Control-Max-Age"] = "86400"

    req_hdrs = (request.headers.get("Access-Control-Request-Headers") or "").strip()
    if req_hdrs:
        resp.headers["Access-Control-Allow-Headers"] = req_hdrs
    else:
        resp.headers["Access-Control-Allow-Headers"] = "*"

    return resp


@bp_marketplace.after_request
def _marketplace_after_request(resp: Response) -> Response:
    return _apply_cors_headers(resp)


@bp_marketplace.route("/vscode/<path:_any>", methods=["OPTIONS"], strict_slashes=False)
def _vscode_preflight(_any: str) -> Response:
    return ("", 204)


def _is_safe_relpath(p: str) -> bool:
    if not p or "\x00" in p:
        return False
    pp = Path(p)
    if pp.is_absolute() or pp.drive:
        return False
    if any(part == ".." for part in pp.parts):
        return False
    return not p.startswith(("/", "\\"))


def _safe_seg(s: str) -> str:
    s2 = (s or "").strip()
    if not s2 or s2 in {".", ".."}:
        raise ValueError("bad segment")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", s2):
        raise ValueError("bad segment")
    return s2


def _norm_ns(s: str) -> str:
    return _safe_seg(s).lower()


def _norm_ext(s: str) -> str:
    return _safe_seg(s).lower()


def _normalize_tp(tp: Optional[str]) -> str:
    v = (tp or "").strip().lower()
    if not v or v == "universal":
        return "universal"
    return v if v in ALLOWED_PLATFORMS else "universal"


def _tp_from_headers_or_path(os_name: Optional[str] = None, arch_name: Optional[str] = None) -> str:
    os_v = (os_name or "").strip().lower()
    arch_v = (arch_name or "").strip().lower()

    if not os_v:
        os_v = (request.headers.get("X-Market-Os") or request.headers.get("X-Market-OS") or "").strip().lower()
    if not arch_v:
        arch_v = (request.headers.get("X-Market-Arch") or request.headers.get("X-Market-ARCH") or "").strip().lower()

    if os_v and arch_v:
        return _normalize_tp(f"{os_v}-{arch_v}")
    return "universal"


def _guess_mimetype(name: str) -> str:
    mt, _enc = mimetypes.guess_type(name, strict=False)
    if not mt:
        return "application/octet-stream"
    mt_l = mt.lower()
    if mt_l.startswith("text/"):
        return f"{mt}; charset=utf-8"
    if mt_l in {"application/json", "application/javascript", "application/xml"}:
        return f"{mt}; charset=utf-8"
    if mt_l == "image/svg+xml":
        return "image/svg+xml; charset=utf-8"
    return mt


def _asset_send_bytes(data: bytes, name: str) -> Response:
    return Response(data, status=200, mimetype=_guess_mimetype(name))


def _zip_read_optional(zf: zipfile.ZipFile, member: str) -> Optional[bytes]:
    try:
        return zf.read(member)
    except KeyError:
        return None


def _zip_children(zf: zipfile.ZipFile, prefix: str) -> Tuple[List[str], List[str]]:
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    dirs: set[str] = set()
    files: set[str] = set()
    for name in zf.namelist():
        if not name.startswith(prefix):
            continue
        rest = name[len(prefix) :]
        if not rest or rest == "/":
            continue
        part = rest.split("/", 1)[0]
        if "/" in rest:
            dirs.add(part)
        else:
            files.add(part)
    return (sorted(dirs), sorted(files))


def _zip_package_json(vsix_path: Path) -> Dict[str, Any]:
    with zipfile.ZipFile(vsix_path, "r") as zf:
        raw = (
            _zip_read_optional(zf, "extension/package.json")
            or _zip_read_optional(zf, "package.json")
            or _zip_read_optional(zf, "extension/extension/package.json")
            or _zip_read_optional(zf, "Extension/package.json")
            or _zip_read_optional(zf, "extension/Package.json")
            or _zip_read_optional(zf, "Package.json")
        )
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}


def _extract_display_name(pkg: Dict[str, Any], fallback: str) -> str:
    v = pkg.get("displayName")
    return v.strip() if isinstance(v, str) and v.strip() else fallback


def _extract_description(pkg: Dict[str, Any]) -> str:
    v = pkg.get("description")
    return v.strip() if isinstance(v, str) else ""


def _extract_tags(pkg: Dict[str, Any]) -> List[str]:
    v = pkg.get("keywords")
    if not isinstance(v, list):
        return []
    out: List[str] = []
    for x in v:
        if isinstance(x, str) and x.strip():
            out.append(x.strip())
    return out


def _extract_categories(pkg: Dict[str, Any]) -> List[str]:
    v = pkg.get("categories")
    if not isinstance(v, list):
        return []
    out: List[str] = []
    for x in v:
        if isinstance(x, str) and x.strip():
            out.append(x.strip())
    return out


def _mk_asset_uri(ns: str, ext: str, ver: str) -> str:
    return f"{_base_url()}/vscode/asset/{ns}/{ext}/{ver}"


def _mk_asset_url(ns: str, ext: str, ver: str, asset_type: str, tp: str) -> str:
    return f"{_base_url()}/vscode/asset/{ns}/{ext}/{ver}/{asset_type}?targetPlatform={tp}"


def _mk_vspackage_url(ns: str, ext: str, ver: str, tp: str) -> str:
    return _mk_asset_url(ns, ext, ver, ASSET_VSIX, tp)


@dataclass(frozen=True)
class ExtRecord:
    namespace: str
    name: str
    version: str
    target_platform: str

    @property
    def vsix_path(self) -> Path:
        return _vsix_path(self.namespace, self.name, self.version, self.target_platform)

    @property
    def unpacked_dir(self) -> Path:
        return _unpacked_dir(self.namespace, self.name, self.version, self.target_platform)


def _to_records(rows) -> List[ExtRecord]:
    out: List[ExtRecord] = []
    for r in rows:
        out.append(ExtRecord(r.namespace, r.name, r.version, r.target_platform))
    return out


def _stable_version(ns: str, ext: str) -> Optional[str]:
    product_dir = _releases_ext_root() / ns / ext
    v = get_latest_version_from_symlinks(product_dir, "latest")
    return v if v else None


def _choose_tp_for_request(tp_requested: Optional[str], os_name: Optional[str] = None, arch_name: Optional[str] = None) -> str:
    tp_req_norm = _normalize_tp(tp_requested)
    if tp_requested and tp_req_norm != "universal":
        return tp_req_norm
    tp_hdr = _tp_from_headers_or_path(os_name=os_name, arch_name=arch_name)
    return tp_hdr if tp_hdr != "universal" else "universal"


def _pick_latest_records(ns: str, ext: str, tp_eff: str) -> Optional[List[ExtRecord]]:
    recs_all = REGISTRY.list_records(ns, ext)
    if not recs_all:
        return None

    stable_ver = _stable_version(ns, ext)
    if stable_ver:
        stable = [r for r in recs_all if r.version == stable_ver and (tp_eff == "universal" or r.target_platform in {tp_eff, "universal"})]
        if stable:
            return _to_records(stable)

    filtered = recs_all if tp_eff == "universal" else [r for r in recs_all if r.target_platform in {tp_eff, "universal"}]
    if not filtered:
        return None
    top_ver = filtered[0].version
    top = [r for r in filtered if r.version == top_ver]
    return _to_records(top)


def _vscode_extension_json(ns: str, ext: str, all_rows, flags: int, tp_eff: str) -> Dict[str, Any]:
    recs_all = _to_records(all_rows)
    if not recs_all:
        return {}

    latest_for_display = recs_all[0]
    pkg = _zip_package_json(latest_for_display.vsix_path) if latest_for_display.vsix_path.is_file() else {}
    display = _extract_display_name(pkg, ext)
    desc = _extract_description(pkg)
    tags = _extract_tags(pkg)
    categories = _extract_categories(pkg)

    include_versions = (flags & FLAG_INCLUDE_VERSIONS) != 0 or (flags & FLAG_INCLUDE_VERSION_PROPERTIES) != 0
    only_latest = (flags & FLAG_INCLUDE_LATEST_VERSION_ONLY) != 0

    chosen: List[ExtRecord] = []
    if include_versions:
        chosen = recs_all[:1] if only_latest else recs_all

    stable_ver = _stable_version(ns, ext)

    versions_json: List[Dict[str, Any]] = []
    for r in chosen:
        asset_uri = _mk_asset_uri(ns, ext, r.version) if (flags & FLAG_INCLUDE_ASSET_URI) != 0 else None
        files_json: Optional[List[Dict[str, Any]]] = None

        if (flags & FLAG_INCLUDE_FILES) != 0:
            ver_tp = _normalize_tp(tp_eff or r.target_platform)
            if ver_tp == "universal":
                ver_tp = _tp_from_headers_or_path()
            files_json = [
                {"assetType": ASSET_VSIX, "source": _mk_vspackage_url(ns, ext, r.version, ver_tp)},
                {"assetType": ASSET_MANIFEST, "source": _mk_asset_url(ns, ext, r.version, ASSET_MANIFEST, ver_tp)},
                {"assetType": ASSET_VSIXMANIFEST, "source": _mk_asset_url(ns, ext, r.version, ASSET_VSIXMANIFEST, ver_tp)},
            ]

        versions_json.append(
            {
                "version": r.version,
                "assetUri": asset_uri,
                "fallbackAssetUri": asset_uri,
                "files": files_json,
                "properties": None,
                "targetPlatform": r.target_platform,
                "isPreReleaseVersion": bool(stable_ver) and r.version != stable_ver,
            }
        )

    return {
        "extensionId": f"{ns}.{ext}",
        "extensionName": ext,
        "displayName": display,
        "shortDescription": desc,
        "publisher": {
            "displayName": ns,
            "publisherId": None,
            "publisherName": ns,
            "domain": None,
            "isDomainVerified": None,
        },
        "versions": versions_json if include_versions else None,
        "statistics": [] if (flags & FLAG_INCLUDE_STATISTICS) != 0 else None,
        "tags": tags if (flags & FLAG_INCLUDE_CATEGORY_AND_TAGS) != 0 else None,
        "categories": categories if (flags & FLAG_INCLUDE_CATEGORY_AND_TAGS) != 0 else None,
        "flags": "",
    }


def _serve_asset(ns: str, ext: str, ver: str, asset_type: str, tp_eff: str, rest: str) -> Response:
    rec_row = REGISTRY.pick_record(ns, ext, ver, tp_eff)
    if not rec_row and tp_eff != "universal":
        rec_row = REGISTRY.pick_record(ns, ext, ver, "universal")
        if rec_row:
            tp_eff = "universal"

    if not rec_row:
        return ("", 404)

    rec = ExtRecord(rec_row.namespace, rec_row.name, rec_row.version, rec_row.target_platform)

    if asset_type == ASSET_VSIX:
        if not rec.vsix_path.is_file():
            return ("", 404)
        download_name = f"{ns}.{ext}-{ver}.vsix"
        return send_file(rec.vsix_path, mimetype="application/octet-stream", as_attachment=False, download_name=download_name)

    if asset_type in {ASSET_DETAILS, ASSET_CHANGELOG, ASSET_LICENSE, ASSET_ICON, ASSET_VSIXMANIFEST, ASSET_MANIFEST}:
        if not rec.vsix_path.is_file():
            return ("", 404)
        with zipfile.ZipFile(rec.vsix_path, "r") as zf:
            if asset_type == ASSET_DETAILS:
                candidates = ["extension/README.md", "README.md", "extension/readme.md"]
            elif asset_type == ASSET_CHANGELOG:
                candidates = ["extension/CHANGELOG.md", "CHANGELOG.md", "extension/changelog.md"]
            elif asset_type == ASSET_LICENSE:
                candidates = ["extension/LICENSE", "LICENSE", "extension/LICENSE.md", "LICENSE.md", "extension/LICENSE.txt", "LICENSE.txt"]
            elif asset_type == ASSET_ICON:
                pkg = _zip_package_json(rec.vsix_path)
                icon = pkg.get("icon")
                candidates2: List[str] = []
                if isinstance(icon, str) and icon.strip():
                    icon_path = icon.strip().lstrip("/")
                    candidates2.append("extension/" + icon_path if not icon_path.startswith("extension/") else icon_path)
                    candidates2.append(icon_path)
                candidates = candidates2 + ["extension/icon.png", "icon.png", "extension/icon.svg", "icon.svg", "extension/icon.jpg", "icon.jpg"]
            elif asset_type == ASSET_VSIXMANIFEST:
                candidates = ["extension.vsixmanifest", "extension/extension.vsixmanifest", "Extension.vsixmanifest", "extension/Extension.vsixmanifest"]
            else:
                candidates = [
                    "extension/package.json",
                    "package.json",
                    "extension/extension/package.json",
                    "Extension/package.json",
                    "extension/Package.json",
                    "Package.json",
                ]

            for c in candidates:
                b = _zip_read_optional(zf, c)
                if b is not None:
                    return _asset_send_bytes(b, Path(c).name)
        return ("", 404)

    if asset_type == ASSET_WEB_RESOURCES:
        rel = (rest or "").lstrip("/")
        if not rel or not _is_safe_relpath(rel):
            return ("", 404)
        if not rec.vsix_path.is_file():
            return ("", 404)
        want = "extension/" + rel if not rel.startswith("extension/") else rel
        with zipfile.ZipFile(rec.vsix_path, "r") as zf:
            b = _zip_read_optional(zf, want)
            if b is None:
                return ("", 404)
            return _asset_send_bytes(b, Path(want).name)

    return ("", 404)


def _atomic_write_bytes(dst: Path, data: bytes) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + f".tmp.{int(time.time() * 1000)}")
    tmp.write_bytes(data)
    tmp.replace(dst)


def _read_vsix_package_json_bytes(vsix_bytes: bytes) -> Dict[str, Any]:
    with zipfile.ZipFile(io.BytesIO(vsix_bytes), "r") as zf:
        raw = (
            _zip_read_optional(zf, "extension/package.json")
            or _zip_read_optional(zf, "package.json")
            or _zip_read_optional(zf, "extension/extension/package.json")
            or _zip_read_optional(zf, "Extension/package.json")
            or _zip_read_optional(zf, "extension/Package.json")
            or _zip_read_optional(zf, "Package.json")
        )
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}


@dataclass(frozen=True)
class _UploadMeta:
    namespace: str
    name: str
    version: str


def _extract_upload_meta(vsix_bytes: bytes) -> _UploadMeta:
    pkg = _read_vsix_package_json_bytes(vsix_bytes)
    ns = pkg.get("publisher")
    name = pkg.get("name")
    ver = pkg.get("version")
    if not isinstance(ns, str) or not ns.strip():
        raise ValueError("missing publisher")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("missing name")
    if not isinstance(ver, str) or not ver.strip():
        raise ValueError("missing version")
    return _UploadMeta(namespace=_norm_ns(ns), name=_norm_ext(name), version=ver.strip())


def _published_at_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@bp_marketplace.post("/api/user/publish")
def publish_extension_openvsx() -> Tuple[Response, int]:
    if "file" not in request.files:
        return jsonify({"state": False, "status": "error", "error": "Missing file"}), 400

    f = request.files["file"]
    if not f:
        return jsonify({"state": False, "status": "error", "error": "Missing file"}), 400

    vsix_bytes = f.read()
    if not vsix_bytes or len(vsix_bytes) < 4 or vsix_bytes[:2] != b"PK":
        return jsonify({"state": False, "status": "error", "error": "Invalid VSIX"}), 400

    try:
        tp = _normalize_tp(request.form.get("targetPlatform"))
        meta = _extract_upload_meta(vsix_bytes)
    except Exception:
        return jsonify({"state": False, "status": "error", "error": "Invalid VSIX metadata"}), 400

    tp_dir = _ext_dir(meta.namespace, meta.name, meta.version, tp)
    vsix_path = tp_dir / "extension.vsix"

    try:
        _atomic_write_bytes(vsix_path, vsix_bytes)
    except Exception:
        return jsonify({"state": False, "status": "error", "error": "Write failed"}), 500

    try:
        REGISTRY.init_and_rebuild()
    except Exception:
        return jsonify({"state": False, "status": "error", "error": "index_rebuild_failed"}), 500

    payload = {
        "state": True,
        "status": "success",
        "data": {
            "namespace": meta.namespace,
            "name": meta.name,
            "version": meta.version,
            "targetPlatform": tp,
            "path": str(vsix_path),
            "publishedAt": _published_at_iso(),
        },
    }
    return jsonify(payload), 201


@bp_marketplace.route("/vscode/gallery/extensionquery", methods=["POST", "OPTIONS"], strict_slashes=False)
def extensionquery() -> Response:
    if request.method == "OPTIONS":
        return ("", 204)

    param = request.get_json(silent=True) or {}
    filters = param.get("filters") or []
    flags_in = int(param.get("flags") or 0)
    flags = flags_in

    page_number = 1
    page_size = 20
    tp_req = "universal"
    ext_id: Optional[str] = None
    search_text: Optional[str] = None

    if isinstance(filters, list) and filters:
        f0 = filters[0] or {}
        try:
            page_number = int(f0.get("pageNumber") or 1)
        except Exception:
            page_number = 1
        try:
            page_size = int(f0.get("pageSize") or 20)
        except Exception:
            page_size = 20
        crit = f0.get("criteria") or []
        if isinstance(crit, list):
            for c in crit:
                if not isinstance(c, dict):
                    continue
                ft = c.get("filterType")
                val = c.get("value")
                if ft in {FILTER_EXTENSION_NAME, FILTER_EXTENSION_ID} and isinstance(val, str) and val.strip():
                    ext_id = val.strip()
                if ft in {FILTER_SEARCH_TEXT, FILTER_TAG} and isinstance(val, str) and val.strip() and not search_text:
                    search_text = val.strip()
                if ft == FILTER_TARGET and isinstance(val, str) and val.strip():
                    tp_req = val.strip()

    is_specific = bool(ext_id)

    if is_specific:
        flags = flags & (~FLAG_INCLUDE_LATEST_VERSION_ONLY)
        flags |= (FLAG_INCLUDE_VERSIONS | FLAG_INCLUDE_FILES | FLAG_INCLUDE_ASSET_URI | FLAG_INCLUDE_VERSION_PROPERTIES)

    tp_eff = _choose_tp_for_request(tp_req)
    offset = max(0, page_number - 1) * max(1, page_size)

    if _log.isEnabledFor(logging.INFO):
        _log.info(
            "extensionquery reqId=%s specific=%s ext_id=%s search=%s tp_req=%s tp_eff=%s flags_in=%s flags_eff=%s page=%s size=%s",
            getattr(g, "request_id", None),
            is_specific,
            ext_id,
            search_text,
            tp_req,
            tp_eff,
            flags_in,
            flags,
            page_number,
            page_size,
        )

    extensions: List[Dict[str, Any]] = []
    total = 0

    if ext_id:
        if "." in ext_id:
            ns_s, name_s = ext_id.split(".", 1)
        else:
            ns_s, name_s = "", ext_id
        try:
            ns = _norm_ns(ns_s)
            ext = _norm_ext(name_s)
        except Exception:
            ns, ext = "", ""

        if ns and ext:
            rows = REGISTRY.list_records(ns, ext)
            if tp_eff != "universal":
                rows = [r for r in rows if r.target_platform in {tp_eff, "universal"}]
            total = 1 if rows else 0
            if rows:
                e = _vscode_extension_json(ns, ext, rows, flags, tp_eff)
                extensions = [e]
                if _log.isEnabledFor(logging.INFO):
                    vcnt = 0
                    try:
                        vcnt = len(e.get("versions") or [])
                    except Exception:
                        vcnt = -1
                    _log.info(
                        "extensionquery respId=%s ext=%s.%s rows=%s versions=%s",
                        getattr(g, "request_id", None),
                        ns,
                        ext,
                        len(rows),
                        vcnt,
                    )
    else:
        pairs = REGISTRY.list_pairs(search_text=search_text)
        total = len(pairs)
        sliced = pairs[offset : offset + page_size]
        for ns, ext in sliced:
            rows = REGISTRY.list_records(ns, ext)
            if tp_eff != "universal":
                rows = [r for r in rows if r.target_platform in {tp_eff, "universal"}]
            if rows:
                extensions.append(_vscode_extension_json(ns, ext, rows, flags, tp_eff))

    payload = {
        "results": [
            {
                "extensions": extensions,
                "resultMetadata": [{"metadataType": "ResultCount", "metadataItems": [{"name": "TotalCount", "count": int(total)}]}],
            }
        ]
    }
    return jsonify(payload)


@bp_marketplace.route("/vscode/gallery/<namespaceName>/<extensionName>/latest", methods=["GET", "OPTIONS"], strict_slashes=False)
def latest(namespaceName: str, extensionName: str) -> Response:
    if request.method == "OPTIONS":
        return ("", 204)

    try:
        ns = _norm_ns(namespaceName)
        ext = _norm_ext(extensionName)
    except Exception:
        return ("", 404)

    tp_req = request.args.get("targetPlatform")
    tp_eff = _choose_tp_for_request(tp_req)

    flags = (
        FLAG_INCLUDE_VERSIONS
        | FLAG_INCLUDE_ASSET_URI
        | FLAG_INCLUDE_VERSION_PROPERTIES
        | FLAG_INCLUDE_FILES
        | FLAG_INCLUDE_STATISTICS
    )

    all_rows = REGISTRY.list_records(ns, ext)
    if not all_rows:
        return ("", 404)

    picked = _pick_latest_records(ns, ext, tp_eff)
    if not picked:
        return ("", 404)

    ver = picked[0].version
    rows_for_ver = [r for r in all_rows if r.version == ver]
    if tp_eff != "universal":
        rows_for_ver = [r for r in rows_for_ver if r.target_platform in {tp_eff, "universal"}]

    if not rows_for_ver:
        return ("", 404)

    return jsonify(_vscode_extension_json(ns, ext, rows_for_ver, flags, tp_eff))


@bp_marketplace.route(
    "/vscode/gallery/asset/<namespaceName>/<extensionName>/<version>/<osName>/<archName>/<assetType>/",
    defaults={"rest": ""},
    methods=["GET", "OPTIONS"],
    strict_slashes=False,
)
@bp_marketplace.route(
    "/vscode/gallery/asset/<namespaceName>/<extensionName>/<version>/<osName>/<archName>/<assetType>/<path:rest>",
    methods=["GET", "OPTIONS"],
    strict_slashes=False,
)
def gallery_asset(namespaceName: str, extensionName: str, version: str, osName: str, archName: str, assetType: str, rest: str) -> Response:
    if request.method == "OPTIONS":
        return ("", 204)

    try:
        ns = _norm_ns(namespaceName)
        ext = _norm_ext(extensionName)
        ver = (version or "").strip()
        if not ver:
            raise ValueError("bad version")
    except Exception:
        return ("", 404)

    tp_eff = _choose_tp_for_request(request.args.get("targetPlatform"), os_name=osName, arch_name=archName)
    return _serve_asset(ns, ext, ver, assetType, tp_eff, rest)


@bp_marketplace.route(
    "/vscode/asset/<namespaceName>/<extensionName>/<version>/<assetType>",
    defaults={"rest": ""},
    methods=["GET", "OPTIONS"],
    strict_slashes=False,
)
@bp_marketplace.route(
    "/vscode/asset/<namespaceName>/<extensionName>/<version>/<assetType>/",
    defaults={"rest": ""},
    methods=["GET", "OPTIONS"],
    strict_slashes=False,
)
@bp_marketplace.route(
    "/vscode/asset/<namespaceName>/<extensionName>/<version>/<assetType>/<path:rest>",
    methods=["GET", "OPTIONS"],
    strict_slashes=False,
)
def get_asset(namespaceName: str, extensionName: str, version: str, assetType: str, rest: str) -> Response:
    if request.method == "OPTIONS":
        return ("", 204)

    try:
        ns = _norm_ns(namespaceName)
        ext = _norm_ext(extensionName)
        ver = (version or "").strip()
        if not ver:
            raise ValueError("bad version")
    except Exception:
        return ("", 404)

    tp_eff = _choose_tp_for_request(request.args.get("targetPlatform"))
    return _serve_asset(ns, ext, ver, assetType, tp_eff, rest)


@bp_marketplace.route("/vscode/unpkg/<namespaceName>/<extensionName>/<version>/", defaults={"path": ""}, methods=["GET", "OPTIONS"], strict_slashes=False)
@bp_marketplace.route("/vscode/unpkg/<namespaceName>/<extensionName>/<version>/<path:path>", methods=["GET", "OPTIONS"], strict_slashes=False)
def unpkg(namespaceName: str, extensionName: str, version: str, path: str) -> Response:
    if request.method == "OPTIONS":
        return ("", 204)

    try:
        ns = _norm_ns(namespaceName)
        ext = _norm_ext(extensionName)
        ver = (version or "").strip()
        if not ver:
            raise ValueError("bad version")
    except Exception:
        return ("", 404)

    tp_eff = _choose_tp_for_request(request.args.get("targetPlatform"))
    rec_row = REGISTRY.pick_record(ns, ext, ver, tp_eff)
    if not rec_row and tp_eff != "universal":
        rec_row = REGISTRY.pick_record(ns, ext, ver, "universal")

    if not rec_row:
        return ("", 404)

    rec = ExtRecord(rec_row.namespace, rec_row.name, rec_row.version, rec_row.target_platform)

    rel = (path or "").lstrip("/")
    if rel != "" and not _is_safe_relpath(rel):
        abort(400, "Invalid path")

    if rec.unpacked_dir.is_dir():
        target = rec.unpacked_dir / rel
        if target.is_dir():
            items: List[str] = []
            for child in sorted(target.iterdir(), key=lambda p: p.name):
                child_rel = (rel + "/" if rel else "") + child.name
                url = f"{_base_url()}/vscode/unpkg/{ns}/{ext}/{ver}/{child_rel}"
                if child.is_dir():
                    url += "/"
                items.append(url)
            return jsonify(items)
        if target.is_file():
            return send_file(target, mimetype=_guess_mimetype(target.name), as_attachment=False, download_name=target.name)
        return ("", 404)

    if not rec.vsix_path.is_file():
        return ("", 404)

    with zipfile.ZipFile(rec.vsix_path, "r") as zf:
        if rel and not rel.endswith("/") and rel in zf.namelist():
            return _asset_send_bytes(zf.read(rel), Path(rel).name)

        prefix = rel
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        dirs, files = _zip_children(zf, prefix)

        if not dirs and not files and rel:
            if rel in zf.namelist():
                return _asset_send_bytes(zf.read(rel), Path(rel).name)
            return ("", 404)

        base = f"{_base_url()}/vscode/unpkg/{ns}/{ext}/{ver}/"
        items: List[str] = []
        for d in dirs:
            items.append(base + prefix + d + "/")
        for f in files:
            items.append(base + prefix + f)
        return jsonify(items)
