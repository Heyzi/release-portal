#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence


# -----------------------------
# Demo project configuration
# -----------------------------


@dataclass(frozen=True)
class DemoProject:
    id: str
    versions: Sequence[str]
    # platforms: "universal" => files in version root
    platforms: Sequence[str]


DEMO_CATEGORIES: Dict[str, List[DemoProject]] = {
    "ide": [
        DemoProject(
            id="myide",
            versions=["1.0.0", "1.1.0", "2.0.0-rc1", "2.0.0"],
            platforms=["universal", "linux-x64", "win32-x64"],
        ),
        DemoProject(
            id="anotheride",
            versions=["0.5.0"],
            platforms=["win32-x64"],
        ),
    ],
    # extensions: intentionally disabled (do not seed)
}


# -----------------------------
# Helpers for files/artifacts
# -----------------------------


def pick_ext_for_platform(platform: str) -> str:
    """
    Minimal extension selection per platform.
    Only for realistic artifact names.
    """
    if platform.startswith("win32"):
        return ".msi"
    if platform.startswith("darwin"):
        return ".dmg"
    if platform.startswith("alpine"):
        return ".tar.gz"
    # linux-* fallback
    return ".tar.gz"


def write_dummy_file(path: Path, size_bytes: int) -> None:
    """
    Creates a file of a given size so human_size() in the app
    shows different size values.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    block = b"0" * 1024
    remaining = size_bytes
    with path.open("wb") as f:
        while remaining > 0:
            chunk = block if remaining >= len(block) else block[:remaining]
            f.write(chunk)
            remaining -= len(chunk)


def write_demo_notes(vdir: Path, project_id: str, version: str, version_index: int) -> None:
    """
    Создаёт markdown-файл заметок в корне релиза.
    Имя файла варьируем, чтобы продемонстрировать поддержку
    разных вариантов регистра и имён (release.md / Release.MD / readme.md).
    """
    note_variants = ["release.md", "Release.MD", "readme.md"]
    notes_name = note_variants[version_index % len(note_variants)]
    notes_path = vdir / notes_name

    lines = [
        f"# Release notes for {project_id} {version}",
        "",
        f"- Demo seeded at epoch: {int(time.time())}",
        "- This is a demo release notes file used by the release portal UI.",
        "- You can replace this file with your real `release.md`.",
        "",
        "## Changes",
        f"- Feature #{version_index + 1}: demo change description.",
        "- Various internal improvements and bug fixes.",
    ]
    notes_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def create_release(
    root: Path,
    project_id: str,
    version: str,
    platforms: Sequence[str],
    version_index: int,
) -> None:
    """
    Creates structure:
      <root>/<project>/<version>[/<platform>]/files...
    Writes RELEASED_AT, artifacts, .sha256 sidecars and demo release notes.
    """
    vdir = root / project_id / version
    vdir.mkdir(parents=True, exist_ok=True)

    # RELEASED_AT: make older timestamps per version so ordering looks realistic
    released_at = int(time.time()) - version_index * 86400
    (vdir / "RELEASED_AT").write_text(str(released_at), encoding="utf-8")

    # Universal artifacts (version root)
    if "universal" in platforms:
        universal_name = f"{project_id}-{version}.zip"
        universal_path = vdir / universal_name
        write_dummy_file(universal_path, size_bytes=2 * 1024 * 1024)  # ~2 MB

        sha_path = vdir / (universal_name + ".sha256")
        sha_path.write_text("dummy-checksum-universal\n", encoding="utf-8")

    # Platform-specific artifacts
    for platform in platforms:
        if platform == "universal":
            continue

        plat_dir = vdir / platform
        plat_dir.mkdir(parents=True, exist_ok=True)

        ext = pick_ext_for_platform(platform)
        artifact_name = f"{project_id}-{version}-{platform}{ext}"
        artifact_path = plat_dir / artifact_name

        size_map: Dict[int, int] = {
            0: 5 * 1024 * 1024,   # 5 MB
            1: 15 * 1024 * 1024,  # 15 MB
            2: 40 * 1024 * 1024,  # 40 MB
        }
        size_bytes = size_map.get(version_index % len(size_map), 5 * 1024 * 1024)
        write_dummy_file(artifact_path, size_bytes=size_bytes)

        sidecar = plat_dir / (artifact_name + ".sha256")
        sidecar.write_text("dummy-checksum-platform\n", encoding="utf-8")

    write_demo_notes(vdir=vdir, project_id=project_id, version=version, version_index=version_index)


# -----------------------------
# Main logic
# -----------------------------


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed demo projects/releases for the release portal.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("./data/releases"),
        help="Root directory for releases (default: ./data/releases)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Remove existing content under root before seeding",
    )
    return parser.parse_args(argv)


def ensure_clean_root(root: Path, force: bool) -> None:
    if root.exists():
        if any(root.iterdir()) and not force:
            print(
                f"ERROR: {root} already exists and is not empty. "
                "Use --force to wipe it before seeding.",
                file=sys.stderr,
            )
            sys.exit(1)
        if force:
            for child in root.iterdir():
                if child.is_dir():
                    import shutil

                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink()
    else:
        root.mkdir(parents=True, exist_ok=True)


def seed(root: Path) -> None:
    for cat_id, projects in DEMO_CATEGORIES.items():
        print(f"Creating category {cat_id!r} in {root}")
        for project in projects:
            print(f"  - project {project.id!r}")
            for idx, ver in enumerate(project.versions):
                print(f"    - version {ver!r}")
                create_release(
                    root=root / cat_id,
                    project_id=project.id,
                    version=ver,
                    platforms=project.platforms,
                    version_index=idx,
                )


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    root: Path = args.root

    ensure_clean_root(root, force=args.force)
    seed(root)
    print(f"\nDone. Seeded demo releases under: {root}")
    print("You can now start your Flask app and open /ui or /admin.")


if __name__ == "__main__":
    main()
