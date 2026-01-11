# ide_api.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from flask import Blueprint, abort, jsonify, request, send_from_directory, url_for

from releases_service import (
    RELEASES_ROOT,
    UNIVERSAL_PLATFORM,
    build_projects_only,
    build_releases_for_project,
    ensure_latest_exists,
    is_safe_relpath,
    normalize_platform,
    strict_pick_latest_symlink,
)

bp_ide = Blueprint("ide_api", __name__)


@bp_ide.route("/api/ide/projects", methods=["GET"])
def ide_projects():
    """
    Returns IDE projects list in the same envelope style as your existing /api/projects.
    """
    cats = build_projects_only()
    ide = next((c for c in cats if c.get("id") == "ide"), {"id": "ide", "name": "Ide", "projects": []})
    return jsonify({"state": True, "status": "success", "data": ide}), 200


@bp_ide.route("/api/ide/releases", methods=["GET"])
def ide_releases():
    """
    Returns releases for an IDE project.
    Query:
      - project (required)
    """
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

    rels = build_releases_for_project("ide", project)
    return jsonify({"state": True, "status": "success", "data": {"category": "ide", "project": project, "releases": rels}}), 200


@bp_ide.route("/api/ide/releases/file/<path:path>", methods=["GET"])
def ide_release_file(path: str):
    """
    Same as /api/releases/file/<path>, but nested under /api/ide for clarity.
    """
    if not is_safe_relpath(path):
        abort(400, "Invalid path")
    return send_from_directory(str(RELEASES_ROOT), path, as_attachment=True)


@bp_ide.route("/api/ide/latest", methods=["GET"])
def ide_latest():
    """
    IDE latest endpoint with the same contract/fields as your current /<category>/latest.

    Query:
      - sub_product_name (required)
      - os_type, arch (optional but must be provided together)
      - current_version (optional)
    """
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

    latest_ver = ensure_latest_exists(pd, "ide")
    if not latest_ver:
        return (
            jsonify({"state": False, "status": "error", "errorType": "file_not_found", "errorMessage": "No releases found"}),
            424,
        )

    platform = UNIVERSAL_PLATFORM
    if os_raw and arch_raw:
        platform = normalize_platform(os_raw, arch_raw)

    link = strict_pick_latest_symlink(pd, platform)
    if not link:
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

    file_name = link.name
    latest_rel_path = (
        f"ide/{project}/latest/{file_name}"
        if platform == UNIVERSAL_PLATFORM
        else f"ide/{project}/latest/{platform}/{file_name}"
    )

    latest_url = f"{request.scheme}://{request.host}{url_for('api_release_file', path=latest_rel_path)}"

    data_obj: Dict[str, Any] = {
        "url": latest_url,
        "sub_product_name": project,
        "available": True,
        "version": latest_ver,
        "requested_current_version": current_version_param or None,
        "platform": None if platform == UNIVERSAL_PLATFORM else platform,
    }

    return jsonify({"state": True, "status": "success", "data": data_obj, "result": data_obj}), 200
