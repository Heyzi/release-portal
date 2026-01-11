# app.py
from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import Flask, abort, jsonify, redirect, render_template, request, send_from_directory, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.routing import BaseConverter

from logging_setup import configure_logging

# OpenVSX blueprints:
# - bp_public: OpenVSX-compatible /api/... endpoints
from openvsx_api import bp_public as openvsx_public_bp

# IDE API blueprint (/api/ide/...)
from ide_api import bp_ide as ide_bp

# Service-layer imports
from releases_service import (  # noqa: F401
    RELEASES_ROOT,
    UNIVERSAL_PLATFORM,
    ensure_latest_exists,
    get_latest_version_from_symlinks,
    is_safe_relpath,
    normalize_platform,
    set_latest_atomic,
    strict_pick_latest_symlink,
    list_versions,
    list_categories,
    build_projects_only,
    build_releases_for_project,
    set_releases_root,
    clear_dir_files_only,
    unlink_if_exists,
)

LOG = configure_logging()

app = Flask(__name__)


# -----------------------------
# URL converter for regex routes
# -----------------------------
class RegexConverter(BaseConverter):
    def __init__(self, map, *items):
        super().__init__(map)
        self.regex = items[0]


app.url_map.converters["re"] = RegexConverter

# Register blueprints AFTER converter
# IMPORTANT:
# - We DO NOT register legacy /extensions/* endpoints anymore.
# - Only public OpenVSX-compatible endpoints under /api/... remain.
app.register_blueprint(openvsx_public_bp, url_prefix="")

# IDE endpoints under /api/ide/...
app.register_blueprint(ide_bp, url_prefix="")

app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)  # type: ignore[method-assign]


# -----------------------------
# API docs pages
# -----------------------------
def _iter_endpoints(prefixes: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint == "static":
            continue
        path = str(rule.rule)
        if not any(path == p or path.startswith(p + "/") for p in prefixes):
            continue
        methods = sorted(m for m in (rule.methods or set()) if m not in {"HEAD", "OPTIONS"})
        rows.append(
            {
                "path": path,
                "methods_str": ", ".join(methods),
                "endpoint": rule.endpoint,
            }
        )
    rows.sort(key=lambda r: (r["path"], r["methods_str"], r["endpoint"]))
    return rows


@app.route("/api", methods=["GET"])
def api_docs():
    rows = _iter_endpoints(prefixes=["/api"])
    return render_template(
        "api_docs.html",
        title="API endpoints",
        subtitle="Доступные эндпоинты /api",
        scope_label="/api",
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        rows=rows,
    )


@app.route("/admin/help", methods=["GET"])
def admin_docs():
    rows = _iter_endpoints(prefixes=["/admin"])
    return render_template(
        "api_docs.html",
        title="Admin endpoints",
        subtitle="Доступные эндпоинты /admin",
        scope_label="/admin",
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        rows=rows,
    )


# -----------------------------
# Existing release portal API (generic)
# You can keep it, but IDE now has a clean /api/ide/* surface.
# -----------------------------
@app.route("/api/releases/file/<path:path>", methods=["GET"])
def api_release_file(path: str):
    if not is_safe_relpath(path):
        abort(400, "Invalid path")
    return send_from_directory(str(RELEASES_ROOT), path, as_attachment=True)


@app.route("/api/projects", methods=["GET"])
def api_projects():
    return jsonify({"state": True, "status": "success", "data": build_projects_only()}), 200


@app.route("/api/releases", methods=["GET"])
def api_releases():
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
    return jsonify(
        {"state": True, "status": "success", "data": {"category": category, "project": project, "releases": rels}}
    ), 200


# -----------------------------
# UI
# -----------------------------
def render_portal(is_admin: bool):
    categories: List[Dict[str, Any]] = build_projects_only()

    selected_category: Optional[str] = (request.args.get("category") or "").strip()
    if selected_category and all(c["id"] != selected_category for c in categories):
        selected_category = None
    if not selected_category and categories:
        selected_category = categories[0]["id"]

    selected_project_id: Optional[str] = (request.args.get("project") or "").strip()
    selected_cat_projects: List[Dict[str, Any]] = next(
        (c["projects"] for c in categories if c["id"] == selected_category),
        [],
    )
    if selected_project_id and all(p["id"] != selected_project_id for p in selected_cat_projects):
        selected_project_id = None
    if not selected_project_id and selected_cat_projects:
        selected_project_id = selected_cat_projects[0]["id"]

    return render_template(
        "index.html",
        categories=categories,
        selected_category=selected_category,
        selected_project_id=selected_project_id,
        is_admin=is_admin,
    )


@app.route("/ui", methods=["GET"])
def ui():
    return render_portal(is_admin=False)


@app.route("/admin", methods=["GET"])
def admin():
    return render_portal(is_admin=True)


# -----------------------------
# Admin actions (unchanged)
# -----------------------------
@app.route("/admin/delete-project", methods=["POST"])
def admin_delete_project():
    category = (request.form.get("category") or "").strip()
    project = (request.form.get("project") or "").strip()
    if not category or not project:
        abort(400, "Missing category/project")

    pd = RELEASES_ROOT / category / project
    if not pd.is_dir():
        abort(400, "Unknown project")

    try:
        shutil.rmtree(pd)
        LOG.info("Deleted project", extra={"category": category, "project": project})
    except Exception as exc:
        LOG.warning("Failed deleting project %s: %s", pd, exc)
        abort(500, "Failed to delete project")

    return redirect(url_for("admin", category=category))


@app.route("/admin/make-latest", methods=["POST"])
def admin_make_latest():
    category = (request.form.get("category") or "").strip()
    project = (request.form.get("project") or "").strip()
    version = (request.form.get("version") or "").strip()
    if not category or not project or not version:
        abort(400, "Missing category/project/version")

    pd = RELEASES_ROOT / category / project
    if not pd.is_dir():
        abort(400, "Unknown project")

    if not (pd / version).is_dir() or version.lower() == "latest":
        abort(400, "Unknown version")

    try:
        set_latest_atomic(pd, version)
        LOG.info("Set latest version", extra={"category": category, "project": project, "version": version})
    except Exception as exc:
        LOG.warning(
            "make-latest failed: category=%s project=%s version=%s err=%s", category, project, version, exc
        )
        abort(500, "Failed to set latest")

    return redirect(url_for("admin", category=category, project=project))


@app.route("/admin/delete-release", methods=["POST"])
def admin_delete_release():
    category = (request.form.get("category") or "").strip()
    project = (request.form.get("project") or "").strip()
    version = (request.form.get("version") or "").strip()
    if not category or not project or not version:
        abort(400, "Missing category/project/version")

    pd = RELEASES_ROOT / category / project
    if not pd.is_dir():
        abort(400, "Unknown project")

    if version.lower() == "latest":
        abort(400, "Cannot delete latest")

    vdir = pd / version
    if not vdir.is_dir():
        abort(400, "Unknown version")

    current_latest = get_latest_version_from_symlinks(pd)

    try:
        shutil.rmtree(vdir)
        LOG.info("Deleted release", extra={"category": category, "project": project, "version": version})
    except Exception as exc:
        LOG.warning("Failed deleting %s: %s", vdir, exc)
        abort(500, "Failed to delete release")

    if current_latest and current_latest == version:
        remaining = list_versions(pd, category)
        if remaining:
            try:
                set_latest_atomic(pd, remaining[0])
                LOG.info(
                    "Repointed latest after delete",
                    extra={"category": category, "project": project, "new_latest": remaining[0]},
                )
            except Exception as exc:
                LOG.warning("Failed to repoint latest after delete: %s", exc)
        else:
            clear_dir_files_only(pd / "latest")

    return redirect(url_for("admin", category=category, project=project))


@app.route("/admin/upload-notes", methods=["POST"])
def admin_upload_notes():
    category = (request.form.get("category") or "").strip()
    project = (request.form.get("project") or "").strip()
    version = (request.form.get("version") or "").strip()
    if not category or not project or not version:
        abort(400, "Missing category/project/version")

    pd = RELEASES_ROOT / category / project
    if not pd.is_dir():
        abort(400, "Unknown project")

    if version.lower() == "latest":
        abort(400, "Cannot upload notes for latest")

    vdir = pd / version
    if not vdir.is_dir():
        abort(400, "Unknown version")

    if "notes" not in request.files:
        abort(400, "Missing notes file")

    file = request.files["notes"]
    if not file or not file.filename:
        abort(400, "Missing notes file")

    target = vdir / "release.md"
    try:
        vdir.mkdir(parents=True, exist_ok=True)
        file.save(str(target))
        LOG.info(
            "Uploaded release notes",
            extra={"category": category, "project": project, "version": version, "path": str(target)},
        )
    except Exception as exc:
        LOG.warning(
            "Failed to upload notes: category=%s project=%s version=%s err=%s", category, project, version, exc
        )
        abort(500, "Failed to upload notes")

    return redirect(url_for("admin", category=category, project=project))


@app.route("/admin/delete-asset", methods=["POST"])
def admin_delete_asset():
    category = (request.form.get("category") or "").strip()
    project = (request.form.get("project") or "").strip()
    version = (request.form.get("version") or "").strip()
    platform = (request.form.get("platform") or "").strip()
    name = (request.form.get("name") or "").strip()
    if not category or not project or not version or not name:
        abort(400, "Missing category/project/version/name")

    pd = RELEASES_ROOT / category / project
    if not pd.is_dir():
        abort(400, "Unknown project")

    if version.lower() == "latest":
        abort(400, "Cannot delete asset for latest")

    vdir = pd / version
    if not vdir.is_dir():
        abort(400, "Unknown version")

    target_dir = vdir / platform if platform else vdir
    target = target_dir / name
    if not target.is_file():
        abort(400, "Asset not found")

    try:
        unlink_if_exists(target)
        LOG.info(
            "Deleted asset",
            extra={
                "category": category,
                "project": project,
                "version": version,
                "platform": platform,
                "asset_name": name,
            },
        )
    except Exception as exc:
        LOG.warning("Failed to delete asset: %s err=%s", target, exc)
        abort(500, "Failed to delete asset")

    current_latest = get_latest_version_from_symlinks(pd)
    if current_latest == version:
        set_latest_atomic(pd, version)

    return redirect(url_for("admin", category=category, project=project))


# -----------------------------
# CLI / Entry
# -----------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Release portal")
    parser.add_argument("--releases-root", help=f"Root directory for releases (default: {RELEASES_ROOT})")
    parser.add_argument("--host", default="0.0.0.0", help="Host for Flask app")
    parser.add_argument("--port", type=int, default=8000, help="Port for Flask app")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    if args.releases_root:
        set_releases_root(Path(args.releases_root))
    app.run(host=args.host, port=args.port, debug=args.debug)
