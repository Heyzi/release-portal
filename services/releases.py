from __future__ import annotations

import os
import re
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from packaging.version import InvalidVersion, Version  # type: ignore
except Exception:  # pragma: no cover
    Version = None  # type: ignore
    InvalidVersion = Exception  # type: ignore


# =========================
# Release notes (merged from notes.py)
# =========================

def read_release_notes(version_dir: Path) -> Optional[Tuple[str, str, str, str]]:
    """
    Returns (filename, notes_text, notes_html, notes_format) where notes_format is 'html' or 'text'.
    Notes HTML is returned as-is if the file is .html; otherwise it's the raw text (no markdown rendering).
    """
    if not version_dir.is_dir():
        return None

    candidates = [
        "RELEASE.md",
        "RELEASE.txt",
        "RELEASE.html",
        "release.md",
        "release.txt",
        "release.html",
        "NOTES.md",
        "NOTES.txt",
    ]

    for name in candidates:
        p = version_dir / name
        if not p.is_file():
            continue

        try:
            raw = p.read_text(encoding="utf-8")
        except Exception:
            continue

        fmt = "html" if p.suffix.lower() == ".html" else "text"
        return (p.name, raw, raw, fmt)

    return None


# =========================
# Releases root directory
# =========================

RELEASES_ROOT = Path("./data/releases")

# Categories used by the portal
CATEGORIES = ["ide", "extensions", "tools"]

# Universal platform marker
UNIVERSAL_PLATFORM = "universal"

# Canonical platform mapping
CANONICAL_PLATFORMS: Dict[Tuple[str, str], str] = {
    ("windows", "x86-64"): "win32-x64",
    ("windows", "x86_64"): "win32-x64",
    ("windows", "amd64"): "win32-x64",
    ("windows", "arm64"): "win32-arm64",
    ("linux", "x86-64"): "linux-x64",
    ("linux", "x86_64"): "linux-x64",
    ("linux", "amd64"): "linux-x64",
    ("linux", "arm64"): "linux-arm64",
    ("linux", "armhf"): "linux-armhf",
    ("alpine", "x86-64"): "alpine-x64",
    ("alpine", "x86_64"): "alpine-x64",
    ("alpine", "amd64"): "alpine-x64",
    ("alpine", "arm64"): "alpine-arm64",
    ("darwin", "x86-64"): "darwin-x64",
    ("darwin", "x86_64"): "darwin-x64",
    ("darwin", "amd64"): "darwin-x64",
    ("darwin", "arm64"): "darwin-arm64",
}

# Asset names excluded from lists
EXCLUDED_ASSET_NAMES_LOWER = {"readme.md", "release.md"}


def set_releases_root(path: Path) -> None:
    # Update global releases root
    global RELEASES_ROOT
    RELEASES_ROOT = path


def normalize_os(os_raw: str) -> str:
    # Normalize OS strings
    x = os_raw.strip().lower()
    if x in ("win", "windows", "win32"):
        return "windows"
    if x in ("mac", "macos", "osx", "darwin"):
        return "darwin"
    if x == "linux":
        return "linux"
    if x == "alpine":
        return "alpine"
    return x


def normalize_arch(arch_raw: str) -> str:
    # Normalize arch strings
    x = arch_raw.strip().lower()
    if x in ("x64", "x86_64", "x86-64", "amd64"):
        return "x86-64"
    if x in ("arm64", "aarch64"):
        return "arm64"
    if x in ("armhf", "armv7", "armv7l"):
        return "armhf"
    return x


def normalize_platform(os_raw: str, arch_raw: str) -> str:
    # Convert OS+arch to canonical platform string
    os_key = normalize_os(os_raw)
    arch_key = normalize_arch(arch_raw)
    return CANONICAL_PLATFORMS.get((os_key, arch_key), f"{os_key}-{arch_key}")


_semverish_re = re.compile(
    r"^v?(?P<num>\d+(?:\.\d+)*)(?:-(?P<pre>[0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$"
)


def _semverish_key(v: str) -> Tuple:
    # Fallback semver-ish sort key
    s = v.strip()
    m = _semverish_re.match(s)
    if not m:
        return (0, s.lower())
    nums = [int(x) for x in m.group("num").split(".")]
    pre = m.group("pre")
    is_final = 1 if pre is None else 0
    pre_key = ("",) if pre is None else tuple(pre.lower().split("."))
    return (1, tuple(nums), is_final, pre_key)


def parse_version_key(version: str) -> Tuple:
    # Prefer packaging.version when available
    if Version is not None:
        try:
            return (2, Version(version.strip().lstrip("v")))
        except InvalidVersion:
            return (1,) + _semverish_key(version)
    return (1,) + _semverish_key(version)


def is_safe_relpath(p: str) -> bool:
    # Validate relative path
    if not p or "\x00" in p:
        return False
    pp = Path(p)
    if pp.is_absolute() or pp.drive:
        return False
    if any(part == ".." for part in pp.parts):
        return False
    return not p.startswith(("/", "\\"))


def human_size(num_bytes: int) -> str:
    # Render human-readable file size
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KB"
    if num_bytes < 1024 * 1024 * 1024:
        return f"{num_bytes / (1024 * 1024):.1f} MB"
    return f"{num_bytes / (1024 * 1024 * 1024):.1f} GB"


def list_dirs(p: Path) -> List[Path]:
    # List directories safely
    try:
        return [x for x in p.iterdir() if x.is_dir()]
    except OSError:
        return []


def _is_excluded_asset_name(name: str) -> bool:
    # Check excluded asset names
    return name.strip().lower() in EXCLUDED_ASSET_NAMES_LOWER


def list_files_assets(p: Path) -> List[Path]:
    # List asset files excluding known non-assets
    try:
        out: List[Path] = []
        for x in p.iterdir():
            if x.is_file() and not _is_excluded_asset_name(x.name):
                out.append(x)
        return out
    except OSError:
        return []


def unlink_if_exists(p: Path) -> None:
    # Remove file or symlink if present
    try:
        if p.is_symlink() or p.is_file():
            p.unlink()
    except OSError:
        pass


def clear_dir_files_only(p: Path) -> None:
    # Remove only files inside directory recursively, keep directories
    if not p.is_dir():
        return
    for child in p.iterdir():
        if child.is_dir():
            clear_dir_files_only(child)
        else:
            unlink_if_exists(child)


def _published_epoch_from_dir(vdir: Path) -> Optional[str]:
    # Use directory mtime as "published at"
    try:
        return str(int(vdir.stat().st_mtime))
    except OSError:
        return None


def list_categories() -> List[str]:
    # List categories present on disk
    return [c for c in CATEGORIES if (RELEASES_ROOT / c).is_dir()]


def list_projects_for_category(category: str) -> List[str]:
    # List projects under a category (non-extensions)
    cat_dir = RELEASES_ROOT / category
    if not cat_dir.is_dir():
        return []
    return sorted([p.name for p in list_dirs(cat_dir) if p.name.lower() != "latest"])


def list_versions(product_dir: Path, category: str) -> List[str]:
    # List versions for a product
    versions = [
        p.name
        for p in list_dirs(product_dir)
        if p.name.lower() not in {"latest", "latest-prerelease"}
    ]
    if category == "tools":
        return sorted(
            versions,
            key=lambda v: int(_published_epoch_from_dir(product_dir / v) or "0"),
            reverse=True,
        )
    return sorted(versions, key=parse_version_key, reverse=True)


def extract_version_from_symlink_target(product_dir: Path, link: Path) -> Optional[str]:
    # Extract a version from symlink target path parts
    try:
        raw_target = os.readlink(str(link))
    except OSError:
        return None
    for part in Path(raw_target).parts:
        if part not in (".", "..", "latest", "latest-prerelease") and (product_dir / part).is_dir():
            return part
    return None


def get_latest_version_from_symlinks(product_dir: Path, latest_name: str = "latest") -> Optional[str]:
    # Read latest version from latest symlink tree
    latest_root = product_dir / latest_name
    if not latest_root.is_dir():
        return None
    for p in latest_root.rglob("*"):
        if p.is_symlink():
            v = extract_version_from_symlink_target(product_dir, p)
            if v:
                return v
    return None


def _rel_target(target: Path, link_dir: Path) -> Path:
    # Create relative symlink targets
    return Path(os.path.relpath(str(target), start=str(link_dir)))


def set_latest_atomic(product_dir: Path, version: str, latest_name: str = "latest") -> None:
    # Atomically rebuild a latest symlink tree
    ver_dir = product_dir / version
    if not ver_dir.is_dir():
        raise FileNotFoundError(version)

    ts = str(int(time.time() * 1000))
    latest_root = product_dir / latest_name
    tmp = product_dir / f".{latest_name}_tmp_{ts}"
    tmp.mkdir(parents=True, exist_ok=True)

    for f in list_files_assets(ver_dir):
        (tmp / f.name).symlink_to(_rel_target(f, tmp))

    for plat_dir in list_dirs(ver_dir):
        tp = tmp / plat_dir.name
        tp.mkdir(parents=True, exist_ok=True)
        for f in list_files_assets(plat_dir):
            (tp / f.name).symlink_to(_rel_target(f, tp))

    if latest_root.exists():
        shutil.rmtree(latest_root)
    tmp.rename(latest_root)


def ensure_latest_exists(product_dir: Path, category: str) -> Optional[str]:
    # Ensure stable latest exists for ide/tools, but not for extensions
    versions = list_versions(product_dir, category)
    if not versions:
        return None

    current = get_latest_version_from_symlinks(product_dir, "latest")
    if current and (product_dir / current).is_dir():
        return current

    if category == "extensions":
        return None

    set_latest_atomic(product_dir, versions[0], latest_name="latest")
    return versions[0]


def strict_pick_latest_symlink(product_dir: Path, platform: str, latest_name: str = "latest") -> Optional[Path]:
    # Pick one symlink from latest tree deterministically
    latest_root = product_dir / latest_name
    if not latest_root.is_dir():
        return None

    if platform == UNIVERSAL_PLATFORM:
        syms = [p for p in latest_root.iterdir() if p.is_symlink()]
    else:
        plat_dir = latest_root / platform
        if not plat_dir.is_dir():
            return None
        syms = [p for p in plat_dir.iterdir() if p.is_symlink()]

    return sorted(syms, key=lambda x: x.name)[0] if syms else None


def build_projects_only() -> List[Dict[str, Any]]:
    # Build portal projects list
    result: List[Dict[str, Any]] = []

    for cat in list_categories():
        projects: List[Dict[str, Any]] = []

        if cat == "extensions":
            cat_dir = RELEASES_ROOT / cat
            for ns_dir in list_dirs(cat_dir):
                ns = ns_dir.name
                if ns.lower() == "latest":
                    continue
                for ext_dir in list_dirs(ns_dir):
                    ext = ext_dir.name
                    if ext.lower() == "latest":
                        continue
                    versions = list_versions(ext_dir, cat)
                    projects.append(
                        {
                            "id": f"{ns}/{ext}",
                            "name": f"{ns}.{ext}",
                            "description": None,
                            "releases_count": len(versions),
                        }
                    )
            projects.sort(key=lambda x: (str(x.get("name") or ""), str(x.get("id") or "")))
        else:
            for proj in list_projects_for_category(cat):
                pd = RELEASES_ROOT / cat / proj
                versions = list_versions(pd, cat)
                ensure_latest_exists(pd, cat)
                projects.append(
                    {
                        "id": proj,
                        "name": proj,
                        "description": None,
                        "releases_count": len(versions),
                    }
                )
            projects.sort(key=lambda x: (str(x.get("name") or ""), str(x.get("id") or "")))

        result.append({"id": cat, "name": cat.capitalize(), "projects": projects})

    return result


def build_releases_for_project(category: str, project: str) -> List[Dict[str, Any]]:
    # Build portal releases list for a project
    vendor: Optional[str] = None
    ext: Optional[str] = None

    if category == "extensions":
        parts = [p for p in (project or "").split("/") if p]
        if len(parts) != 2:
            return []
        vendor, ext = parts[0], parts[1]
        if vendor.lower() == "latest" or ext.lower() == "latest":
            return []
        pd = RELEASES_ROOT / category / vendor / ext
    else:
        pd = RELEASES_ROOT / category / project

    if not pd.is_dir():
        return []

    versions = list_versions(pd, category)
    latest_ver = ensure_latest_exists(pd, category)

    releases: List[Dict[str, Any]] = []

    for ver in versions:
        vdir = pd / ver
        assets: List[Dict[str, Any]] = []

        def rel_path_for(file_path: Path, platform: Optional[str] = None) -> str:
            # Build storage-relative path for an asset
            if category == "extensions" and vendor and ext:
                if platform:
                    return f"{category}/{vendor}/{ext}/{ver}/{platform}/{file_path.name}"
                return f"{category}/{vendor}/{ext}/{ver}/{file_path.name}"
            if platform:
                return f"{category}/{project}/{ver}/{platform}/{file_path.name}"
            return f"{category}/{project}/{ver}/{file_path.name}"

        for f in list_files_assets(vdir):
            assets.append(
                {
                    "name": f.name,
                    "size": human_size(f.stat().st_size),
                    "href": rel_path_for(f),
                    "platform": None,
                }
            )

        for plat_dir in list_dirs(vdir):
            for f in list_files_assets(plat_dir):
                assets.append(
                    {
                        "name": f.name,
                        "size": human_size(f.stat().st_size),
                        "href": rel_path_for(f, platform=plat_dir.name),
                        "platform": plat_dir.name,
                    }
                )

        notes = read_release_notes(vdir)
        releases.append(
            {
                "tag": ver,
                "title": None,
                "published_at": _published_epoch_from_dir(vdir),
                "is_latest": bool(latest_ver) and ver == latest_ver,
                "assets": assets,
                "notes_name": notes[0] if notes else None,
                "notes": notes[1] if notes else None,
                "notes_html": notes[2] if notes else None,
                "notes_format": notes[3] if notes else None,
            }
        )

    return releases
