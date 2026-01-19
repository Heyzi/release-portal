# api/releases_api.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from flask import Blueprint, abort, current_app, jsonify, render_template, request, send_from_directory

from services.releases import RELEASES_ROOT, build_projects_only, build_releases_for_project, is_safe_relpath

bp_releases = Blueprint("releases_api", __name__)


def _iter_endpoints(app, prefixes: List[str]) -> List[Dict[str, Any]]:
    # Enumerate endpoints for docs pages
    rows: List[Dict[str, Any]] = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint == "static":
            continue
        path = str(rule.rule)
        if not any(path == p or path.startswith(p + "/") for p in prefixes):
            continue
        methods = sorted(m for m in (rule.methods or set()) if m not in {"HEAD", "OPTIONS"})
        rows.append({"path": path, "methods_str": ", ".join(methods), "endpoint": rule.endpoint})
    rows.sort(key=lambda r: (r["path"], r["methods_str"], r["endpoint"]))
    return rows


@bp_releases.get("/api")
def api_docs():
    # Render simple API docs page
    rows = _iter_endpoints(current_app, prefixes=["/api"])
    return render_template(
        "api_docs.html",
        title="API endpoints",
        subtitle="Доступные эндпоинты /api",
        scope_label="/api",
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        rows=rows,
    )


@bp_releases.get("/api/releases/file/<path:path>")
def api_release_file(path: str):
    # Serve release file by relative path
    if not is_safe_relpath(path):
        abort(400, "Invalid path")
    return send_from_directory(str(RELEASES_ROOT), path, as_attachment=True)


@bp_releases.get("/api/projects")
def api_projects():
    # Return projects tree
    return jsonify({"state": True, "status": "success", "data": build_projects_only()}), 200


@bp_releases.get("/api/releases")
def api_releases():
    # Return releases for category/project
    category = (request.args.get("category") or "").strip()
    project = (request.args.get("project") or "").strip()
    if not category or not project:
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "invalid_parameters",
                    "errorMessage": "Missing required parameters: category and project",
                }
            ),
            400,
        )

    pd = RELEASES_ROOT / category / project
    if not pd.is_dir():
        return (
            jsonify(
                {
                    "state": False,
                    "status": "error",
                    "errorType": "file_not_found",
                    "errorMessage": "Unknown category/project",
                }
            ),
            424,
        )

    rels = build_releases_for_project(category, project)
    return (
        jsonify(
            {
                "state": True,
                "status": "success",
                "data": {"category": category, "project": project, "releases": rels},
            }
        ),
        200,
    )
