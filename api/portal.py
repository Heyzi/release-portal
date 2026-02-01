# api/portal.py
from __future__ import annotations

import json
import shutil
import time
import uuid
from datetime import datetime
from pathlib import Path
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
    normalize_platform,
    set_latest_atomic,
    unlink_if_exists,
)

# Indexes
from services.extensions_registry import REGISTRY
from services.ide_registry import IDE_REGISTRY

bp_portal = Blueprint("portal", __name__)


def _safe_seg(s: str) -> bool:
    # filesystem segment safety (admin-controlled but still validate)
    if not s or s in {".", ".."}:
        return False
    if any(ch in s for ch in ("\x00", "/", "\\")):
        return False
    return True


def _maybe_rebuild_indexes(category: str) -> None:
    """
    Best-effort index rebuild after filesystem mutations.
    Per TZ: after IDE mutations must call IDE_REGISTRY.init_and_rebuild().
    """
    cat = (category or "").strip().lower()
    try:
        if cat == "ide":
            IDE_REGISTRY.init_and_rebuild()
        elif cat == "extensions":
            REGISTRY.init_and_rebuild()
    except Exception:
        # do not break admin flows if index rebuild fails
        pass


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

    _maybe_rebuild_indexes(category)

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

    _maybe_rebuild_indexes(category)

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

    _maybe_rebuild_indexes(category)

    return redirect(url_for("portal.admin", category=category, project=project))


@bp_portal.post("/admin/upload-notes")
def admin_upload_notes():
    # Upload release notes as release.md into version directory (generic; not IDE changelog)
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

    # Notes do not affect IDE/extension sqlite indexes

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

    _maybe_rebuild_indexes(category)

    return redirect(url_for("portal.admin", category=category, project=project))

@bp_portal.post("/admin/ide/create-project")
def admin_ide_create_project():
    project = (request.form.get("project") or "").strip()
    if not project:
        abort(400, "Missing project")

    if not _safe_seg(project):
        abort(400, "Invalid project name")

    pd = RELEASES_ROOT / "ide" / project
    if pd.exists():
        abort(409, "Project already exists")

    try:
        pd.mkdir(parents=True, exist_ok=False)
    except Exception:
        abort(500, "Failed to create project")

    _maybe_rebuild_indexes("ide")
    return redirect(url_for("portal.admin", category="ide", project=project))

@bp_portal.post("/admin/ide/upload")
def admin_ide_upload():
    """
    IDE artifact upload via admin UI (multipart/form-data).

    Matches TZ Web upload API semantics:
      - files: binary (required), meta (required, json), changelog (optional, markdown)
      - meta filename must be binary filename + ".json"
      - meta required fields: sub_product_name, version, os_type, arch
      - platform = normalize_platform(os_type, arch)
      - write to: ide/<project>/<version>/<platform>/<binary> and <binary>.json
      - changelog (if present) -> ide/<project>/<version>/changelog.md
      - atomic write: temp dir + rename
      - after successful write: IDE_REGISTRY.init_and_rebuild()
    """
    category = "ide"

    if "binary" not in request.files or "meta" not in request.files:
        abort(400, "Missing required files: binary and meta")

    binary = request.files["binary"]
    meta = request.files["meta"]
    changelog = request.files.get("changelog")

    if not binary or not binary.filename or not meta or not meta.filename:
        abort(400, "Missing binary/meta filenames")

    # Enforce naming rule (TZ)
    if meta.filename != f"{binary.filename}.json":
        abort(400, "meta filename must equal binary filename + '.json'")

    # Read meta JSON as UTF-8
    try:
        meta_text = meta.stream.read().decode("utf-8")
    except Exception:
        abort(400, "Failed to read meta as UTF-8")

    try:
        meta_obj = json.loads(meta_text)
    except Exception:
        abort(400, "meta is not valid JSON")

    if not isinstance(meta_obj, dict):
        abort(400, "meta must be a JSON object")

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
    except ValueError as e:
        abort(400, str(e))

    if not _safe_seg(project) or not _safe_seg(version):
        abort(400, "Invalid project/version")

    platform = normalize_platform(os_type, arch)
    if not _safe_seg(platform):
        abort(400, "Invalid platform")

    # Destinations
    dest_dir = RELEASES_ROOT / "ide" / project / version / platform
    dest_bin = dest_dir / binary.filename
    dest_meta = dest_dir / meta.filename
    dest_changelog = RELEASES_ROOT / "ide" / project / version / "changelog.md"

    # Conflict policy: disallow overwriting binary/meta; allow overwriting changelog
    if dest_bin.exists() or dest_meta.exists():
        abort(409, "Artifact already exists")

    tmp_root = RELEASES_ROOT / "_tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)
    tmp_dir = tmp_root / f"ide_upload_{int(time.time())}_{uuid.uuid4().hex}"

    try:
        tmp_dir.mkdir(parents=True, exist_ok=False)

        tmp_plat_dir = tmp_dir / "payload" / "ide" / project / version / platform
        tmp_plat_dir.mkdir(parents=True, exist_ok=True)

        # Save binary + meta
        binary.save(str(tmp_plat_dir / binary.filename))
        (tmp_plat_dir / meta.filename).write_text(meta_text, encoding="utf-8")

        # Save changelog (optional, UTF-8)
        if changelog and changelog.filename:
            try:
                cl_text = changelog.stream.read().decode("utf-8")
            except Exception:
                abort(400, "Failed to read changelog as UTF-8")
            tmp_ver_dir = tmp_dir / "payload" / "ide" / project / version
            tmp_ver_dir.mkdir(parents=True, exist_ok=True)
            (tmp_ver_dir / "changelog.md").write_text(cl_text, encoding="utf-8")

        # Commit (atomic-ish): ensure dirs exist then replace files
        dest_dir.mkdir(parents=True, exist_ok=True)
        (tmp_plat_dir / binary.filename).replace(dest_bin)
        (tmp_plat_dir / meta.filename).replace(dest_meta)

        if changelog and changelog.filename:
            dest_changelog.parent.mkdir(parents=True, exist_ok=True)
            (tmp_dir / "payload" / "ide" / project / version / "changelog.md").replace(dest_changelog)

    except Exception as e:
        # best-effort cleanup
        try:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        # preserve HTTP status if abort was used inside
        if hasattr(e, "code"):
            raise
        abort(500, "Failed to write files")
    finally:
        try:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

    _maybe_rebuild_indexes(category)

    # Redirect back to admin UI (project selected)
    return redirect(url_for("portal.admin", category="ide", project=project))
