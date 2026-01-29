# api/ide.py
from __future__ import annotations

from typing import Any, Dict, Optional

from flask import Blueprint, abort, jsonify, request, send_from_directory, url_for

from services.releases import (
    RELEASES_ROOT,
    UNIVERSAL_PLATFORM,
    get_latest_version_from_symlinks,
    is_safe_relpath,
    normalize_platform,
    strict_pick_latest_symlink,
)


bp_ide = Blueprint("ide_api", __name__)


@bp_ide.get("/api/ide/releases")
def ide_releases():
    # List releases for IDE project
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
                {"state": False, "status": "error", "errorType": "file_not_found", "errorMessage": "Unknown project"}
            ),
            424,
        )

    versions = [p.name for p in pd.iterdir() if p.is_dir() and p.name != "latest"]
    versions = sorted(versions, reverse=True)

    stable_latest = get_latest_version_from_symlinks(pd, "latest")

    releases = []
    for ver in versions:
        vdir = pd / ver
        releases.append(
            {
                "tag": ver,
                "published_at": str(int(vdir.stat().st_mtime)) if vdir.exists() else None,
                "is_latest": bool(stable_latest) and ver == stable_latest,
            }
        )

    return (
        jsonify({"state": True, "status": "success", "data": {"category": "ide", "project": project, "releases": releases}}),
        200,
    )


@bp_ide.get("/api/ide/releases/file/<path:path>")
def ide_release_file(path: str):
    # Download a release file
    if not is_safe_relpath(path):
        abort(400, "Invalid path")
    return send_from_directory(str(RELEASES_ROOT), path, as_attachment=True)


@bp_ide.get("/api/ide/latest")
def ide_latest():
    # Return latest IDE artifact URL for given platform
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

    if bool(os_raw) ^ bool(arch_raw):
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "Both os_type and arch must be provided together",
                }
            ),
            400,
        )

    latest_ver = get_latest_version_from_symlinks(pd, "latest")
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

    platform = UNIVERSAL_PLATFORM
    if os_raw and arch_raw:
        platform = normalize_platform(os_raw, arch_raw)

    picked = strict_pick_latest_symlink(pd, platform, latest_name="latest")
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

    file_name = picked.name
    latest_rel_path = (
        f"ide/{project}/latest/{file_name}"
        if platform == UNIVERSAL_PLATFORM
        else f"ide/{project}/latest/{platform}/{file_name}"
    )
    latest_url = f"{request.scheme}://{request.host}{url_for('releases_api.api_release_file', path=latest_rel_path)}"

    data_obj: Dict[str, Any] = {
        "url": latest_url,
        "sub_product_name": project,
        "available": True,
        "version": latest_ver,
        "requested_current_version": current_version_param or None,
        "platform": None if platform == UNIVERSAL_PLATFORM else platform,
    }

    return jsonify({"state": True, "status": "success", "data": data_obj, "result": data_obj}), 200
