# portal.py
from __future__ import annotations

import shutil
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Blueprint, abort, current_app, redirect, render_template, request, url_for

# single source of truth for docs endpoint enumeration
from api.releases_api import _iter_endpoints

from services.releases import (
    RELEASES_ROOT,
    build_projects_only,
    clear_dir_files_only,
    get_latest_version_from_symlinks,
    list_versions,
    set_latest_atomic,
    unlink_if_exists,
)

bp_portal = Blueprint("portal", __name__)


def render_portal(is_admin: bool):
    # Render portal page with category/project selection
    categories: List[Dict[str, Any]] = build_projects_only()

    selected_category: Optional[str] = (request.args.get("category") or "").strip()
    if selected_category and all(c["id"] != selected_category for c in categories):
        selected_category = None
    if not selected_category and categories:
        selected_category = categories[0]["id"]

    selected_project_id: Optional[str] = (request.args.get("project") or "").strip()
    selected_cat_projects: List[Dict[str, Any]] = next(
        (c["projects"] for c in categories if c["id"] == selected_category), []
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


@bp_portal.get("/")
def root():
    # Redirect root to UI
    return redirect(url_for("portal.ui"))


@bp_portal.get("/ui")
def ui():
    # Public portal
    return render_portal(is_admin=False)


@bp_portal.get("/admin")
def admin():
    # Admin portal
    return render_portal(is_admin=True)


@bp_portal.get("/admin/help")
def admin_docs():
    # Render admin endpoints page
    rows = _iter_endpoints(current_app, prefixes=["/admin"])
    return render_template(
        "api_docs.html",
        title="Admin endpoints",
        subtitle="Доступные эндпоинты /admin",
        scope_label="/admin",
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        rows=rows,
    )


@bp_portal.post("/admin/delete-project")
def admin_delete_project():
    # Delete an entire project directory
    category = (request.form.get("category") or "").strip()
    project = (request.form.get("project") or "").strip()
    if not category or not project:
        abort(400, "Missing category/project")

    pd = RELEASES_ROOT / category / project
    if not pd.is_dir():
        abort(400, "Unknown project")

    try:
        shutil.rmtree(pd)
    except Exception:
        abort(500, "Failed to delete project")

    return redirect(url_for("portal.admin", category=category))


@bp_portal.post("/admin/make-latest")
def admin_make_latest():
    # Set latest symlinks for a project
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
    except Exception:
        abort(500, "Failed to set latest")

    return redirect(url_for("portal.admin", category=category, project=project))


@bp_portal.post("/admin/delete-release")
def admin_delete_release():
    # Delete a single release version directory
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
    except Exception:
        abort(500, "Failed to delete release")

    if current_latest and current_latest == version:
        remaining = list_versions(pd, category)
        if remaining:
            try:
                set_latest_atomic(pd, remaining[0])
            except Exception:
                pass
        else:
            clear_dir_files_only(pd / "latest")

    return redirect(url_for("portal.admin", category=category, project=project))


@bp_portal.post("/admin/upload-notes")
def admin_upload_notes():
    # Upload release notes as release.md into version directory
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
    except Exception:
        abort(500, "Failed to upload notes")

    return redirect(url_for("portal.admin", category=category, project=project))


@bp_portal.post("/admin/delete-asset")
def admin_delete_asset():
    # Delete a single asset file from a release directory
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
    except Exception:
        abort(500, "Failed to delete asset")

    current_latest = get_latest_version_from_symlinks(pd)
    if current_latest == version:
        try:
            set_latest_atomic(pd, version)
        except Exception:
            pass

    return redirect(url_for("portal.admin", category=category, project=project))
