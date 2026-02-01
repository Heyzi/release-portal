# api/ide.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
import time
from flask import Blueprint, Response, abort, jsonify, request, url_for

from services.ide_registry import IDE_REGISTRY
from services.releases import RELEASES_ROOT, is_safe_relpath, normalize_platform

bp_ide = Blueprint("ide_api", __name__)


@bp_ide.get("/api/ide/releases")
def ide_releases():
    # List releases for IDE project (SQLite-backed list; only versions with >=1 valid platform)
    project = (request.args.get("project") or "").strip()
    if not project:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "Missing required parameter: project",
                }
            ),
            400,
        )

    pd = RELEASES_ROOT / "ide" / project
    if not pd.is_dir():
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "file_not_found",
                    "errorMessage": "Unknown project",
                }
            ),
            424,
        )

    versions_rows = IDE_REGISTRY.list_versions(project)
    stable_latest = IDE_REGISTRY.get_stable_latest(project)

    releases = []
    for r in versions_rows:
        releases.append(
            {
                "tag": r.version,
                "published_at": str(int(r.published_ts)) if r.published_ts else None,
                "is_latest": bool(stable_latest) and r.version == stable_latest,
            }
        )

    return (
        jsonify(
            {
                "state": True,
                "status": "success",
                "data": {"category": "ide", "project": project, "releases": releases},
            }
        ),
        200,
    )


@bp_ide.get("/api/ide/latest")
def ide_latest():
    # Return latest IDE artifact URL for given platform (no universal fallback)
    project = (request.args.get("sub_product_name") or "").strip()
    os_raw = (request.args.get("os_type") or "").strip()
    arch_raw = (request.args.get("arch") or "").strip()
    current_version_param = (request.args.get("current_version") or "").strip()

    if not project:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "Missing required parameter: sub_product_name",
                }
            ),
            400,
        )

    pd = RELEASES_ROOT / "ide" / project
    if not pd.is_dir():
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "file_not_found",
                    "errorMessage": "Unknown sub_product_name",
                }
            ),
            424,
        )

    # Per TZ: os_type and arch are required (no universal for IDE)
    if not os_raw or not arch_raw:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "Missing required parameters: os_type and arch",
                }
            ),
            400,
        )

    latest_ver = IDE_REGISTRY.get_stable_latest(project)
    if not latest_ver:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "file_not_found",
                    "errorMessage": "No stable latest set",
                }
            ),
            424,
        )

    platform = normalize_platform(os_raw, arch_raw)

    picked = IDE_REGISTRY.pick_latest_asset(project, platform)
    if not picked:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "file_not_found",
                    "errorMessage": f"No latest artifact for platform={platform}",
                }
            ),
            424,
        )

    binary_rel_path, _file_name = picked

    # Must return URL via /api/releases/file/<binary_rel_path>
    latest_url = f"{request.scheme}://{request.host}{url_for('releases_api.api_release_file', path=binary_rel_path)}"

    data_obj: Dict[str, Any] = {
        "url": latest_url,
        "sub_product_name": project,
        "available": True,
        "version": latest_ver,
        "requested_current_version": current_version_param or None,
        "platform": platform,
    }

    return jsonify({"state": True, "status": "success", "data": data_obj, "result": data_obj}), 200


@bp_ide.get("/api/ide/changelog")
def ide_changelog():
    """
    Return changelog.md for IDE project/version as UTF-8 text.

    Params:
      - project (required)
      - version OR latest=1 (exactly one)
    Returns:
      - 200 text/plain; charset=utf-8
      - 404 if missing
      - 400 on invalid params
    """
    project = (request.args.get("project") or "").strip()
    version = (request.args.get("version") or "").strip()
    latest = (request.args.get("latest") or "").strip()

    if not project:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "Missing required parameter: project",
                }
            ),
            400,
        )

    want_latest = latest in {"1", "true", "yes"}
    if bool(version) == bool(want_latest):
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "Provide exactly one of: version, latest=1",
                }
            ),
            400,
        )

    pd = RELEASES_ROOT / "ide" / project
    if not pd.is_dir():
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "file_not_found",
                    "errorMessage": "Unknown project",
                }
            ),
            424,
        )

    if want_latest:
        v = IDE_REGISTRY.get_stable_latest(project)
        if not v:
            return (
                jsonify(
                    {
                        "state": False,
                        "status": "error",
                        "errorType": "file_not_found",
                        "errorMessage": "No stable latest set",
                    }
                ),
                424,
            )
        version = v

    # Primary TZ storage: ide/<project>/<version>/changelog.md
    p = RELEASES_ROOT / "ide" / project / version / "changelog.md"
    if not p.is_file():
        abort(404, "changelog.md not found")

    try:
        text = p.read_text(encoding="utf-8")
    except Exception:
        abort(500, "Failed to read changelog")

    return Response(text, status=200, mimetype="text/plain", content_type="text/plain; charset=utf-8")


@bp_ide.post("/api/ide/upload")
def ide_upload():
    """
    Web upload API (admin): multipart/form-data

    Files:
      - binary (required)
      - meta (required, json)
      - changelog (optional, markdown)

    Rules:
      - meta.filename must equal binary.filename + ".json"
      - meta content must contain required fields and match project/version/platform (validated by registry rules)
      - save to ide/<project>/<version>/<platform>/<binary>
      - save meta alongside as <binary>.json
      - if changelog present: save to ide/<project>/<version>/changelog.md (overwrite allowed)
      - atomic write: temp dir + rename
      - rebuild IDE index after success
    """
    if "binary" not in request.files or "meta" not in request.files:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "Missing required files: binary and meta",
                }
            ),
            400,
        )

    binary = request.files["binary"]
    meta = request.files["meta"]
    changelog = request.files.get("changelog")

    if not binary or not binary.filename or not meta or not meta.filename:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "Missing binary/meta filenames",
                }
            ),
            400,
        )

    # Enforce naming rule
    if meta.filename != f"{binary.filename}.json":
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "meta filename must equal binary filename + '.json'",
                }
            ),
            400,
        )

    # Read and parse meta JSON (UTF-8)
    try:
        meta_text = meta.stream.read().decode("utf-8")
    except Exception:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "Failed to read meta as UTF-8",
                }
            ),
            400,
        )

    try:
        meta_obj = __import__("json").loads(meta_text)
    except Exception:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "meta is not valid JSON",
                }
            ),
            400,
        )

    if not isinstance(meta_obj, dict):
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "meta must be a JSON object",
                }
            ),
            400,
        )

    def _get_req_str(key: str) -> str:
        v = meta_obj.get(key)
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"Missing required meta field: {key}")
        return v.strip()

    try:
        project = _get_req_str("sub_product_name")
        version = _get_req_str("version")
        os_type = _get_req_str("os_type")
        arch = _get_req_str("arch")
    except Exception as e:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": str(e),
                }
            ),
            400,
        )

    platform = normalize_platform(os_type, arch)

    # Target paths
    dest_dir = RELEASES_ROOT / "ide" / project / version / platform
    dest_bin = dest_dir / binary.filename
    dest_meta = dest_dir / meta.filename
    dest_changelog = RELEASES_ROOT / "ide" / project / version / "changelog.md"

    # Conflict policy: disallow overwriting binary/meta (409). Changelog: overwrite allowed.
    if dest_bin.exists() or dest_meta.exists():
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "conflict",
                    "errorMessage": "Artifact already exists",
                }
            ),
            409,
        )

    # Atomic write via temp dir inside destination version dir
    tmp_root = RELEASES_ROOT / "_tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)
    tmp_dir = tmp_root / f"ide_upload_{int(time.time())}_{__import__('uuid').uuid4().hex}"

    try:
        tmp_dir.mkdir(parents=True, exist_ok=False)

        tmp_dest_dir = tmp_dir / "payload" / "ide" / project / version / platform
        tmp_dest_dir.mkdir(parents=True, exist_ok=True)

        # Save binary/meta
        binary.save(str(tmp_dest_dir / binary.filename))
        (tmp_dest_dir / meta.filename).write_text(meta_text, encoding="utf-8")

        # Save changelog if provided
        if changelog and changelog.filename:
            try:
                cl_text = changelog.stream.read().decode("utf-8")
            except Exception:
                raise ValueError("Failed to read changelog as UTF-8")
            tmp_ver_dir = tmp_dir / "payload" / "ide" / project / version
            tmp_ver_dir.mkdir(parents=True, exist_ok=True)
            (tmp_ver_dir / "changelog.md").write_text(cl_text, encoding="utf-8")

        # Move into place: ensure destination dir exists then rename subtree
        # We rename individual files for simplicity/portability.
        dest_dir.mkdir(parents=True, exist_ok=True)
        (tmp_dest_dir / binary.filename).replace(dest_bin)
        (tmp_dest_dir / meta.filename).replace(dest_meta)

        if changelog and changelog.filename:
            dest_changelog.parent.mkdir(parents=True, exist_ok=True)
            (tmp_dir / "payload" / "ide" / project / version / "changelog.md").replace(dest_changelog)

    except ValueError as e:
        # validation-ish errors
        try:
            if tmp_dir.exists():
                __import__("shutil").rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": str(e),
                }
            ),
            400,
        )
    except Exception:
        try:
            if tmp_dir.exists():
                __import__("shutil").rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "internal_error",
                    "errorMessage": "Failed to write files",
                }
            ),
            500,
        )
    finally:
        try:
            if tmp_dir.exists():
                __import__("shutil").rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

    # Rebuild IDE index after successful write
    try:
        IDE_REGISTRY.init_and_rebuild()
    except Exception:
        # best-effort; API should still succeed
        pass

    # Optional: ensure latest exists if missing (per TZ) — omitted here because ensure_latest_exists
    # is not shown in provided codebase.

    return (
        jsonify(
            {
                "state": True,
                "status": "success",
                "data": {
                    "project": project,
                    "version": version,
                    "platform": platform,
                    "binary_rel_path": f"ide/{project}/{version}/{platform}/{binary.filename}",
                    "meta_rel_path": f"ide/{project}/{version}/{platform}/{meta.filename}",
                    "changelog_written": bool(changelog and changelog.filename),
                },
            }
        ),
        200,
    )
