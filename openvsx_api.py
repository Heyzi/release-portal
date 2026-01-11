# openvsx_api.py
from __future__ import annotations

import io
import json
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, Response, abort, jsonify, request, send_from_directory

from releases_service import (
    RELEASES_ROOT,
    ensure_latest_exists,
    get_latest_version_from_symlinks,
    is_safe_relpath,  # ВАЖНО: берём у вас
    list_versions,
    set_latest_atomic,
)

# Two blueprints:
# - bp_ext: legacy routes mounted under /extensions (keeps your publish endpoint as-is)
# - bp_public: OpenVSX-compatible routes mounted at root (/api/...)
bp_ext = Blueprint("openvsx_ext", __name__)
bp_public = Blueprint("openvsx_public", __name__)

EXTENSIONS_CATEGORY = "extensions"

_ALLOWED_TP = {
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

_TP_REGEX = "(?:%s)" % "|".join(re.escape(x) for x in sorted(_ALLOWED_TP, key=len, reverse=True))
_VER_REGEX = r"(?:\d+(?:\.\d+){1,3}(?:-[0-9A-Za-z.-]+)?)"


def _ext_root() -> Path:
    return RELEASES_ROOT / EXTENSIONS_CATEGORY


def _safe_segment(s: str) -> str:
    s = (s or "").strip()
    if not s or s in (".", ".."):
        raise ValueError("Empty/invalid path segment")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", s):
        raise ValueError(f"Invalid segment: {s}")
    return s


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


@dataclass(frozen=True)
class VsixInfo:
    namespace: str
    name: str
    version: str
    target_platform: str


def _extract_vsix_info(vsix_bytes: bytes) -> VsixInfo:
    with zipfile.ZipFile(io.BytesIO(vsix_bytes)) as zf:
        pkg_path = None
        for cand in ("extension/package.json", "package.json"):
            try:
                zf.getinfo(cand)
                pkg_path = cand
                break
            except KeyError:
                continue
        if not pkg_path:
            raise ValueError("VSIX missing package.json")

        pkg = json.loads(zf.read(pkg_path).decode("utf-8", errors="strict"))
        namespace = _safe_segment(str(pkg.get("publisher", "")).strip())
        name = _safe_segment(str(pkg.get("name", "")).strip())
        version = str(pkg.get("version", "")).strip()
        if not version:
            raise ValueError("VSIX missing version in package.json")

        target_platform = "universal"

        manifest_path = None
        for cand in ("extension.vsixmanifest", "extension/extension.vsixmanifest"):
            try:
                zf.getinfo(cand)
                manifest_path = cand
                break
            except KeyError:
                continue

        if manifest_path:
            manifest_text = zf.read(manifest_path).decode("utf-8", errors="replace")

            # Identity TargetPlatform="linux-x64"
            m_identity = re.search(
                r"<\s*Identity\b[^>]*\bTargetPlatform\s*=\s*\"([^\"]+)\"",
                manifest_text,
                flags=re.IGNORECASE,
            )
            if m_identity:
                tp = m_identity.group(1).strip().lower()
                if tp in _ALLOWED_TP:
                    target_platform = tp

            # Fallback: Property ... Value="linux-x64"
            if target_platform == "universal":
                m_prop = re.search(
                    r'Id\s*=\s*"[^"]*TargetPlatform[^"]*"\s+Value\s*=\s*"([^"]+)"',
                    manifest_text,
                    flags=re.IGNORECASE,
                )
                if m_prop:
                    tp = m_prop.group(1).strip().lower()
                    if tp in _ALLOWED_TP:
                        target_platform = tp

        return VsixInfo(namespace=namespace, name=name, version=version, target_platform=target_platform)


def _store_vsix(vsix_bytes: bytes, info: VsixInfo) -> Dict[str, Any]:
    # Храним VSIX по version/targetPlatform.
    # Latest (ручной/авто) хранится через releases_service.set_latest_atomic() в ext_dir/latest.
    ns, nm, ver, tp = info.namespace, info.name, info.version, info.target_platform

    ext_dir = _ext_root() / ns / nm
    vdir = ext_dir / ver
    pdir = vdir / tp
    _ensure_dir(pdir)

    filename = f"{ns}.{nm}-{ver}.vsix"
    vsix_path = pdir / filename

    tmp = vsix_path.with_suffix(".vsix.tmp")
    tmp.write_bytes(vsix_bytes)
    os.replace(tmp, vsix_path)

    _write_json(
        vdir / "metadata.json",
        {
            "namespace": ns,
            "name": nm,
            "version": ver,
            "targetPlatform": (None if tp == "universal" else tp),
            "files": {"vsix": str(vsix_path.relative_to(RELEASES_ROOT)).replace(os.sep, "/")},
        },
    )

    # Автоматически ставим latest на публикуемую версию.
    # UI (Make latest) может потом переустановить.
    try:
        set_latest_atomic(ext_dir, ver)
    except Exception:
        pass

    return {"namespace": ns, "name": nm, "version": ver, "targetPlatform": tp, "filename": filename}


def _request_base_url() -> str:
    return f"{request.scheme}://{request.host}"


def _openvsx_extension_json(namespace: str, name: str, version: str, tp: str, base: str, public: bool) -> Dict[str, Any]:
    tp = tp or "universal"
    # Public endpoints must be /api/..., legacy stays under /extensions/api/...
    api_prefix = "/api" if public else "/extensions/api"

    if tp != "universal":
        download_path = f"{api_prefix}/{namespace}/{name}/{tp}/{version}/file/{namespace}.{name}-{version}.vsix"
    else:
        download_path = f"{api_prefix}/{namespace}/{name}/{version}/file/{namespace}.{name}-{version}.vsix"

    return {
        "namespace": namespace,
        "name": name,
        "version": version,
        "targetPlatform": (None if tp == "universal" else tp),
        "files": {"download": f"{base}{download_path}"},
    }


def _iter_namespaces() -> List[Path]:
    root = _ext_root()
    if not root.is_dir():
        return []
    try:
        return [p for p in root.iterdir() if p.is_dir()]
    except OSError:
        return []


def _iter_extensions(ns_dir: Path) -> List[Path]:
    try:
        return [p for p in ns_dir.iterdir() if p.is_dir()]
    except OSError:
        return []


def _read_metadata(vdir: Path) -> Optional[Dict[str, Any]]:
    mp = vdir / "metadata.json"
    if not mp.is_file():
        return None
    try:
        return json.loads(mp.read_text(encoding="utf-8"))
    except Exception:
        return None


def _choose_tp_for_version(vdir: Path, requested_tp: Optional[str]) -> Optional[str]:
    # Return best matching target platform:
    # - if requested_tp exists -> use it
    # - else prefer universal if exists, else first allowed dir
    req = (requested_tp or "").strip().lower()
    if req:
        if req in _ALLOWED_TP and (vdir / req).is_dir():
            return req
        # if requested platform is 'universal' but no folder, we still try fallback below.

    if (vdir / "universal").is_dir():
        return "universal"

    try:
        for p in vdir.iterdir():
            if p.is_dir() and p.name in _ALLOWED_TP:
                return p.name
    except OSError:
        return None

    return None


def _pick_default_version(ext_dir: Path) -> Optional[str]:
    # 1) Пиннутый latest из UI (через ext_dir/latest/* symlink tree)
    pinned = get_latest_version_from_symlinks(ext_dir)
    if pinned and (ext_dir / pinned).is_dir():
        return pinned

    # 2) Иначе создаём latest на максимальную (semver) и используем её
    v = ensure_latest_exists(ext_dir, category="extensions")
    if v and (ext_dir / v).is_dir():
        return v

    # 3) Фолбэк
    vs = list_versions(ext_dir, category="extensions")
    return vs[0] if vs else None


def _matches_query(
    ns: str,
    name: str,
    extension_id: str,
    namespace_name: str,
    extension_name: str,
) -> bool:
    if extension_id:
        # Accept both "ns.name" and "ns/name" for convenience; Open VSX uses dot in many clients.
        eid = extension_id.strip()
        eid_norm = eid.replace("/", ".")
        if f"{ns}.{name}".lower() != eid_norm.lower():
            return False

    if namespace_name and ns.lower() != namespace_name.strip().lower():
        return False

    if extension_name and name.lower() != extension_name.strip().lower():
        return False

    return True


# -----------------------------
# PUBLISH (legacy endpoint kept as-is)
# -----------------------------
@bp_ext.route("/admin/extensions/publish", methods=["POST"])
def admin_extensions_publish():
    # nginx is responsible for auth; do not check anything here.

    vsix_bytes: Optional[bytes] = None
    ct = (request.content_type or "").lower()
    if ct.startswith("application/octet-stream") or ct.startswith("application/zip"):
        vsix_bytes = request.get_data(cache=False)
    else:
        f = request.files.get("file")
        if f and f.filename:
            vsix_bytes = f.read()

    if not vsix_bytes:
        return jsonify({"error": "Missing VSIX content"}), 400

    try:
        info = _extract_vsix_info(vsix_bytes)
    except Exception as exc:
        return jsonify({"error": f"Invalid VSIX: {exc}"}), 400

    try:
        stored = _store_vsix(vsix_bytes, info)
    except Exception as exc:
        return jsonify({"error": f"Failed to store VSIX: {exc}"}), 500

    base = _request_base_url()
    ext_json = _openvsx_extension_json(
        stored["namespace"], stored["name"], stored["version"], stored["targetPlatform"], base, public=False
    )
    location = f"{base}/extensions/api/{stored['namespace']}/{stored['name']}/{stored['version']}"
    return Response(
        response=json.dumps(ext_json, ensure_ascii=False),
        status=201,
        mimetype="application/json",
        headers={"Location": location},
    )


# -----------------------------
# PUBLISH (public OpenVSX-compatible alias)
# -----------------------------
@bp_public.route("/api/user/publish", methods=["POST"])
def api_user_publish():
    vsix_bytes: Optional[bytes] = None
    ct = (request.content_type or "").lower()
    if ct.startswith("application/octet-stream") or ct.startswith("application/zip"):
        vsix_bytes = request.get_data(cache=False)
    else:
        f = request.files.get("file")
        if f and f.filename:
            vsix_bytes = f.read()

    if not vsix_bytes:
        return jsonify({"error": "Missing VSIX content"}), 400

    try:
        info = _extract_vsix_info(vsix_bytes)
    except Exception as exc:
        return jsonify({"error": f"Invalid VSIX: {exc}"}), 400

    try:
        stored = _store_vsix(vsix_bytes, info)
    except Exception as exc:
        return jsonify({"error": f"Failed to store VSIX: {exc}"}), 500

    base = _request_base_url()
    ext_json = _openvsx_extension_json(
        stored["namespace"], stored["name"], stored["version"], stored["targetPlatform"], base, public=True
    )
    location = f"{base}/api/{stored['namespace']}/{stored['name']}/{stored['version']}"
    return Response(
        response=json.dumps(ext_json, ensure_ascii=False),
        status=201,
        mimetype="application/json",
        headers={"Location": location},
    )


# -----------------------------
# QUERY (public OpenVSX-compatible)
# -----------------------------
def _handle_query() -> Tuple[Response, int]:
    # Minimal-but-compatible query endpoint for clients:
    # Supports:
    # - extensionId (ns.name), namespaceName, extensionName
    # - extensionVersion
    # - includeAllVersions (true/false)
    # - targetPlatform
    # - size, offset
    extension_id = (request.args.get("extensionId") or "").strip()
    namespace_name = (request.args.get("namespaceName") or "").strip()
    extension_name = (request.args.get("extensionName") or "").strip()
    extension_version = (request.args.get("extensionVersion") or "").strip()
    include_all_versions_raw = (request.args.get("includeAllVersions") or "").strip().lower()
    target_platform = (request.args.get("targetPlatform") or "").strip().lower()

    try:
        size = int((request.args.get("size") or "50").strip())
        offset = int((request.args.get("offset") or "0").strip())
    except ValueError:
        return jsonify({"error": "Invalid size/offset"}), 400

    if size < 1:
        size = 1
    if size > 200:
        size = 200
    if offset < 0:
        offset = 0

    include_all_versions = include_all_versions_raw in ("1", "true", "yes", "on")

    if target_platform and target_platform not in _ALLOWED_TP:
        # Open VSX tends to reject unknown targetPlatform
        return jsonify({"error": f"Invalid targetPlatform: {target_platform}"}), 400

    items: List[Dict[str, Any]] = []
    base = _request_base_url()

    for ns_dir in _iter_namespaces():
        ns = ns_dir.name
        for ext_dir in _iter_extensions(ns_dir):
            name = ext_dir.name

            if not _matches_query(ns, name, extension_id, namespace_name, extension_name):
                continue

            versions = list_versions(ext_dir, category="extensions")

            if extension_version:
                versions = [v for v in versions if v == extension_version]

            versions_payload: List[Dict[str, Any]] = []

            if include_all_versions:
                for ver in versions:
                    vdir = ext_dir / ver
                    tp = _choose_tp_for_version(vdir, target_platform)
                    if not tp:
                        continue
                    versions_payload.append(_openvsx_extension_json(ns, name, ver, tp, base, public=True))
            else:
                ver = _pick_default_version(ext_dir)
                if ver:
                    # если клиент запросил extensionVersion, то latest не должен “перебивать” запрос
                    if extension_version and ver != extension_version:
                        # extension_version уже отфильтрован выше; но на всякий случай:
                        ver = extension_version

                    vdir = ext_dir / ver
                    tp = _choose_tp_for_version(vdir, target_platform)
                    if tp:
                        versions_payload.append(_openvsx_extension_json(ns, name, ver, tp, base, public=True))

            if not versions_payload:
                continue

            if include_all_versions:
                items.append(
                    {
                        "namespace": ns,
                        "name": name,
                        "versions": versions_payload,
                    }
                )
            else:
                # Minimal practical shape: list of top version objects
                items.append(versions_payload[0])

    total = len(items)
    sliced = items[offset : offset + size]

    # Produce minimal QueryResult-like response
    # - For includeAllVersions: "extensions" is list of {namespace,name,versions:[...]}
    # - Else: "extensions" is list of version objects {namespace,name,version,targetPlatform,files:{download}}
    return jsonify({"offset": offset, "totalSize": total, "extensions": sliced}), 200


@bp_public.route("/api/-/query", methods=["GET"])
def api_query():
    return _handle_query()


@bp_public.route("/api/v1/-/query", methods=["GET"])
def api_v1_query():
    return _handle_query()


# -----------------------------
# Namespace endpoint (public + legacy)
# -----------------------------
def _handle_get_namespace(namespace: str) -> Tuple[Response, int]:
    try:
        namespace = _safe_segment(namespace)
    except ValueError:
        return jsonify({"error": "Invalid namespace"}), 400

    ns_dir = _ext_root() / namespace
    if not ns_dir.is_dir():
        return jsonify({"error": f"Namespace not found: {namespace}"}), 404
    return jsonify({"name": namespace}), 200


@bp_ext.route("/api/<namespace>", methods=["GET"])
def get_namespace_legacy(namespace: str):
    return _handle_get_namespace(namespace)


@bp_public.route("/api/<namespace>", methods=["GET"])
def get_namespace_public(namespace: str):
    return _handle_get_namespace(namespace)


# -----------------------------
# Extension metadata (public + legacy)
# -----------------------------
def _handle_get_extension_version(namespace: str, extension: str, version: str, public: bool) -> Tuple[Response, int]:
    try:
        namespace = _safe_segment(namespace)
        extension = _safe_segment(extension)
        version = _safe_segment(version)
    except ValueError:
        return jsonify({"error": "Invalid parameters"}), 400

    vdir = _ext_root() / namespace / extension / version
    if not vdir.is_dir():
        return jsonify({"error": f"Extension not found: {namespace}.{extension}@{version}"}), 404

    tp = _choose_tp_for_version(vdir, requested_tp=None)
    if not tp:
        return jsonify({"error": "No artifacts in version"}), 404

    base = _request_base_url()
    return jsonify(_openvsx_extension_json(namespace, extension, version, tp, base, public=public)), 200


def _handle_get_extension_platform_version(
    namespace: str, extension: str, target_platform: str, version: str, public: bool
) -> Tuple[Response, int]:
    try:
        namespace = _safe_segment(namespace)
        extension = _safe_segment(extension)
        target_platform = _safe_segment(target_platform).lower()
        version = _safe_segment(version)
    except ValueError:
        return jsonify({"error": "Invalid parameters"}), 400

    if target_platform not in _ALLOWED_TP:
        return jsonify({"error": f"Invalid targetPlatform: {target_platform}"}), 400

    pdir = _ext_root() / namespace / extension / version / target_platform
    if not pdir.is_dir():
        return jsonify({"error": f"Extension not found: {namespace}.{extension}@{version} ({target_platform})"}), 404

    base = _request_base_url()
    return jsonify(_openvsx_extension_json(namespace, extension, version, target_platform, base, public=public)), 200


@bp_ext.route(f"/api/<namespace>/<extension>/<re('{_VER_REGEX}'):version>", methods=["GET"])
def get_extension_version_legacy(namespace: str, extension: str, version: str):
    return _handle_get_extension_version(namespace, extension, version, public=False)


@bp_public.route(f"/api/<namespace>/<extension>/<re('{_VER_REGEX}'):version>", methods=["GET"])
def get_extension_version_public(namespace: str, extension: str, version: str):
    return _handle_get_extension_version(namespace, extension, version, public=True)


@bp_ext.route(
    f"/api/<namespace>/<extension>/<re('{_TP_REGEX}'):target_platform>/<re('{_VER_REGEX}'):version>",
    methods=["GET"],
)
def get_extension_platform_version_legacy(namespace: str, extension: str, target_platform: str, version: str):
    return _handle_get_extension_platform_version(namespace, extension, target_platform, version, public=False)


@bp_public.route(
    f"/api/<namespace>/<extension>/<re('{_TP_REGEX}'):target_platform>/<re('{_VER_REGEX}'):version>",
    methods=["GET"],
)
def get_extension_platform_version_public(namespace: str, extension: str, target_platform: str, version: str):
    return _handle_get_extension_platform_version(namespace, extension, target_platform, version, public=True)


# -----------------------------
# File download (public + legacy)
# -----------------------------
def _handle_get_file_universal(namespace: str, extension: str, version: str, filename: str) -> Response:
    try:
        namespace = _safe_segment(namespace)
        extension = _safe_segment(extension)
        version = _safe_segment(version)
    except ValueError:
        return jsonify({"error": "Invalid parameters"}), 400  # type: ignore[return-value]

    rel = Path(EXTENSIONS_CATEGORY) / namespace / extension / version / "universal" / filename
    rel_s = str(rel).replace("\\", "/")
    if not is_safe_relpath(rel_s):
        abort(400, "Invalid path")

    abs_path = RELEASES_ROOT / rel
    if not abs_path.is_file():
        return jsonify({"error": "File not found"}), 404  # type: ignore[return-value]

    return send_from_directory(str(abs_path.parent), abs_path.name, as_attachment=True)


def _handle_get_file(namespace: str, extension: str, target_platform: str, version: str, filename: str) -> Response:
    try:
        namespace = _safe_segment(namespace)
        extension = _safe_segment(extension)
        target_platform = _safe_segment(target_platform).lower()
        version = _safe_segment(version)
    except ValueError:
        return jsonify({"error": "Invalid parameters"}), 400  # type: ignore[return-value]

    if target_platform not in _ALLOWED_TP:
        return jsonify({"error": f"Invalid targetPlatform: {target_platform}"}), 400  # type: ignore[return-value]

    rel = Path(EXTENSIONS_CATEGORY) / namespace / extension / version / target_platform / filename
    rel_s = str(rel).replace("\\", "/")
    if not is_safe_relpath(rel_s):
        abort(400, "Invalid path")

    abs_path = RELEASES_ROOT / rel
    if not abs_path.is_file():
        return jsonify({"error": "File not found"}), 404  # type: ignore[return-value]

    return send_from_directory(str(abs_path.parent), abs_path.name, as_attachment=True)


@bp_ext.route(f"/api/<namespace>/<extension>/<re('{_VER_REGEX}'):version>/file/<path:filename>", methods=["GET"])
def get_file_universal_legacy(namespace: str, extension: str, version: str, filename: str):
    return _handle_get_file_universal(namespace, extension, version, filename)


@bp_public.route(f"/api/<namespace>/<extension>/<re('{_VER_REGEX}'):version>/file/<path:filename>", methods=["GET"])
def get_file_universal_public(namespace: str, extension: str, version: str, filename: str):
    return _handle_get_file_universal(namespace, extension, version, filename)


@bp_ext.route(
    f"/api/<namespace>/<extension>/<re('{_TP_REGEX}'):target_platform>/<re('{_VER_REGEX}'):version>/file/<path:filename>",
    methods=["GET"],
)
def get_file_legacy(namespace: str, extension: str, target_platform: str, version: str, filename: str):
    return _handle_get_file(namespace, extension, target_platform, version, filename)


@bp_public.route(
    f"/api/<namespace>/<extension>/<re('{_TP_REGEX}'):target_platform>/<re('{_VER_REGEX}'):version>/file/<path:filename>",
    methods=["GET"],
)
def get_file_public(namespace: str, extension: str, target_platform: str, version: str, filename: str):
    return _handle_get_file(namespace, extension, target_platform, version, filename)
