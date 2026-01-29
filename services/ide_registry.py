# services/ide_registry.py
from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from services.releases import RELEASES_ROOT, UNIVERSAL_PLATFORM, get_latest_version_from_symlinks


def _indexes_root() -> Path:
    return Path(RELEASES_ROOT) / "_indexes"


def _db_path() -> Path:
    return _indexes_root() / "ide_index.sqlite"


def _ide_root() -> Path:
    return Path(RELEASES_ROOT) / "ide"


def _safe_seg(s: str) -> bool:
    # keep it simple; these are filesystem folder names under our control
    if not s or s in {".", ".."}:
        return False
    if any(ch in s for ch in ("\x00", "/", "\\")):
        return False
    return True


@dataclass(frozen=True)
class IdeAssetRow:
    project: str
    version: str
    platform: str
    file_name: str
    file_rel_path: str
    published_ts: int
    is_latest: int


@dataclass(frozen=True)
class IdeVersionRow:
    version: str
    published_ts: int
    is_latest: bool


class IdeRegistry:
    # SQLite-backed registry for IDE releases
    def __init__(self) -> None:
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self._inited = False

    def _conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is not None:
            return c

        p = _db_path()
        p.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(str(p), timeout=30, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        self._local.conn = conn
        return conn

    def init_and_rebuild(self) -> None:
        # Initialize schema and rebuild from filesystem
        with self._init_lock:
            conn = self._conn()
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ide_assets (
                        project TEXT NOT NULL,
                        version TEXT NOT NULL,
                        platform TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        file_rel_path TEXT NOT NULL,
                        published_ts INTEGER NOT NULL,
                        is_latest INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (project, version, platform, file_name)
                    )
                    """
                )
                conn.execute("DELETE FROM ide_assets")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            rows = self._scan_fs_rows()
            if rows:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.executemany(
                        """
                        INSERT INTO ide_assets(project, version, platform, file_name, file_rel_path, published_ts, is_latest)
                        VALUES(?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                r.project,
                                r.version,
                                r.platform,
                                r.file_name,
                                r.file_rel_path,
                                int(r.published_ts),
                                int(r.is_latest),
                            )
                            for r in rows
                        ],
                    )
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

            conn.execute("CREATE INDEX IF NOT EXISTS idx_ide_project_ver ON ide_assets(project, version)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ide_project_published ON ide_assets(project, published_ts DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ide_project_version_platform ON ide_assets(project, version, platform)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ide_project_latest_platform ON ide_assets(project, is_latest, platform)")

            self._inited = True

    def _scan_fs_rows(self) -> List[IdeAssetRow]:
        root = _ide_root()
        if not root.is_dir():
            return []

        out: List[IdeAssetRow] = []
        now = int(time.time())

        for proj_dir in root.iterdir():
            if not proj_dir.is_dir():
                continue
            project = proj_dir.name.strip()
            if not _safe_seg(project):
                continue

            stable_latest = get_latest_version_from_symlinks(proj_dir, "latest") or ""
            stable_latest = stable_latest.strip()

            for ver_dir in proj_dir.iterdir():
                if not ver_dir.is_dir():
                    continue
                ver = ver_dir.name.strip()
                if not _safe_seg(ver) or ver.lower() == "latest":
                    continue

                # Files directly in version dir => universal
                for child in ver_dir.iterdir():
                    if child.is_file():
                        file_name = child.name
                        if not _safe_seg(file_name):
                            continue
                        try:
                            ts = int(child.stat().st_mtime)
                        except Exception:
                            ts = now
                        rel = f"ide/{project}/{ver}/{file_name}"
                        out.append(
                            IdeAssetRow(
                                project=project,
                                version=ver,
                                platform=UNIVERSAL_PLATFORM,
                                file_name=file_name,
                                file_rel_path=rel,
                                published_ts=ts,
                                is_latest=1 if stable_latest and ver == stable_latest else 0,
                            )
                        )

                # Platform subdirs
                for plat_dir in ver_dir.iterdir():
                    if not plat_dir.is_dir():
                        continue
                    platform = plat_dir.name.strip()
                    if not _safe_seg(platform) or platform.lower() == "latest":
                        continue
                    for f in plat_dir.iterdir():
                        if not f.is_file():
                            continue
                        file_name = f.name
                        if not _safe_seg(file_name):
                            continue
                        try:
                            ts = int(f.stat().st_mtime)
                        except Exception:
                            ts = now
                        rel = f"ide/{project}/{ver}/{platform}/{file_name}"
                        out.append(
                            IdeAssetRow(
                                project=project,
                                version=ver,
                                platform=platform,
                                file_name=file_name,
                                file_rel_path=rel,
                                published_ts=ts,
                                is_latest=1 if stable_latest and ver == stable_latest else 0,
                            )
                        )

        return out

    def _ensure_inited(self) -> None:
        if not self._inited:
            self.init_and_rebuild()

    def get_stable_latest(self, project: str) -> Optional[str]:
        # source of truth remains filesystem "latest" symlinks
        proj = (project or "").strip()
        if not _safe_seg(proj):
            return None
        pd = _ide_root() / proj
        if not pd.is_dir():
            return None
        v = get_latest_version_from_symlinks(pd, "latest")
        return v.strip() if v else None

    def list_versions(self, project: str) -> List[IdeVersionRow]:
        self._ensure_inited()
        proj = (project or "").strip()
        if not _safe_seg(proj):
            return []

        conn = self._conn()
        rows = conn.execute(
            """
            SELECT
                version AS version,
                MAX(published_ts) AS published_ts,
                MAX(is_latest) AS is_latest
            FROM ide_assets
            WHERE project=?
            GROUP BY version
            ORDER BY version DESC
            """,
            (proj,),
        ).fetchall()

        out: List[IdeVersionRow] = []
        for r in rows:
            out.append(
                IdeVersionRow(
                    version=str(r["version"]),
                    published_ts=int(r["published_ts"] or 0),
                    is_latest=bool(int(r["is_latest"] or 0)),
                )
            )
        return out

    def pick_latest_asset(self, project: str, platform: str) -> Optional[Tuple[str, str]]:
        """
        Returns (file_rel_path, file_name) for stable latest of project.
        Selection rules:
        - prefer exact platform match if platform != universal
        - fallback to universal
        - if multiple files, choose most recent (published_ts desc), then name asc
        """
        self._ensure_inited()
        proj = (project or "").strip()
        plat = (platform or "").strip() or UNIVERSAL_PLATFORM
        if not _safe_seg(proj):
            return None
        if not _safe_seg(plat):
            plat = UNIVERSAL_PLATFORM

        conn = self._conn()

        if plat != UNIVERSAL_PLATFORM:
            row = conn.execute(
                """
                SELECT file_rel_path, file_name
                FROM ide_assets
                WHERE project=? AND is_latest=1 AND platform=?
                ORDER BY published_ts DESC, file_name ASC
                LIMIT 1
                """,
                (proj, plat),
            ).fetchone()
            if row:
                return (str(row["file_rel_path"]), str(row["file_name"]))

            row2 = conn.execute(
                """
                SELECT file_rel_path, file_name
                FROM ide_assets
                WHERE project=? AND is_latest=1 AND platform=?
                ORDER BY published_ts DESC, file_name ASC
                LIMIT 1
                """,
                (proj, UNIVERSAL_PLATFORM),
            ).fetchone()
            if row2:
                return (str(row2["file_rel_path"]), str(row2["file_name"]))
            return None

        row3 = conn.execute(
            """
            SELECT file_rel_path, file_name
            FROM ide_assets
            WHERE project=? AND is_latest=1
            ORDER BY published_ts DESC, file_name ASC
            LIMIT 1
            """,
            (proj,),
        ).fetchone()
        if not row3:
            return None
        return (str(row3["file_rel_path"]), str(row3["file_name"]))


IDE_REGISTRY = IdeRegistry()
