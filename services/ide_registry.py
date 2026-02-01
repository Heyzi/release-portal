# services/ide_registry.py
from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

from services.releases import RELEASES_ROOT, get_latest_version_from_symlinks, normalize_platform


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


def _read_json_utf8(p: Path) -> Optional[dict]:
    try:
        raw = p.read_text(encoding="utf-8")
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _req_str(obj: dict, key: str) -> Optional[str]:
    v = obj.get(key)
    if not isinstance(v, str):
        return None
    s = v.strip()
    return s if s else None


@dataclass(frozen=True)
class IdePlatformRow:
    project: str
    version: str
    platform: str
    meta_rel_path: str
    binary_rel_path: str
    published_ts: int
    is_latest: int
    is_valid: int
    invalid_reason: Optional[str]


@dataclass(frozen=True)
class IdeVersionRow:
    version: str
    published_ts: int
    is_latest: bool


class IdeRegistry:
    # SQLite-backed registry for IDE releases (platform-level, per TZ)
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
        """
        Initialize schema and rebuild from filesystem.

        Requirements implemented:
        - index stored at RELEASES_ROOT/_indexes/ide_index.sqlite
        - table ide_platforms with PK(project, version, platform)
        - scan ide/<project>/<version>/<platform>/
        - validation: exactly one *.json, matching binary exists, json required fields,
          normalize_platform(os_type, arch) == platform dir, sub_product_name == project, version == version dir
        - UNIVERSAL_PLATFORM is not used for IDE
        """
        with self._init_lock:
            conn = self._conn()
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ide_platforms (
                        project TEXT NOT NULL,
                        version TEXT NOT NULL,
                        platform TEXT NOT NULL,
                        meta_rel_path TEXT NOT NULL,
                        binary_rel_path TEXT NOT NULL,
                        published_ts INTEGER NOT NULL,
                        is_latest INTEGER NOT NULL,
                        is_valid INTEGER NOT NULL,
                        invalid_reason TEXT NULL,
                        PRIMARY KEY (project, version, platform)
                    )
                    """
                )
                conn.execute("DELETE FROM ide_platforms")
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
                        INSERT INTO ide_platforms(
                            project, version, platform,
                            meta_rel_path, binary_rel_path,
                            published_ts, is_latest, is_valid, invalid_reason
                        )
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                r.project,
                                r.version,
                                r.platform,
                                r.meta_rel_path,
                                r.binary_rel_path,
                                int(r.published_ts),
                                int(r.is_latest),
                                int(r.is_valid),
                                r.invalid_reason,
                            )
                            for r in rows
                        ],
                    )
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

            # Indexes per TZ
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ide_project_ver ON ide_platforms(project, version)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ide_project_latest_platform ON ide_platforms(project, is_latest, platform)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ide_project_published ON ide_platforms(project, published_ts DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ide_project_platform ON ide_platforms(project, platform)")

            self._inited = True

    def _scan_fs_rows(self) -> List[IdePlatformRow]:
        root = _ide_root()
        if not root.is_dir():
            return []

        out: List[IdePlatformRow] = []
        now = int(time.time())

        for proj_dir in root.iterdir():
            if not proj_dir.is_dir():
                continue
            project = proj_dir.name.strip()
            if not _safe_seg(project):
                continue

            stable_latest = (get_latest_version_from_symlinks(proj_dir, "latest") or "").strip()

            for ver_dir in proj_dir.iterdir():
                if not ver_dir.is_dir():
                    continue
                ver = ver_dir.name.strip()
                if not _safe_seg(ver) or ver.lower() == "latest":
                    continue

                # Only platform subdirs are considered. Files directly in version dir are ignored for IDE.
                for plat_dir in ver_dir.iterdir():
                    if not plat_dir.is_dir():
                        continue
                    platform = plat_dir.name.strip()
                    if not _safe_seg(platform) or platform.lower() == "latest":
                        continue

                    meta_files = [p for p in plat_dir.iterdir() if p.is_file() and p.name.endswith(".json")]
                    meta_rel_path = ""
                    binary_rel_path = ""
                    published_ts = now
                    is_latest = 1 if stable_latest and ver == stable_latest else 0
                    is_valid = 0
                    invalid_reason: Optional[str] = None

                    if len(meta_files) == 0:
                        invalid_reason = "no_meta_json"
                    elif len(meta_files) > 1:
                        invalid_reason = "multiple_meta_json"
                    else:
                        meta_p = meta_files[0]
                        meta_name = meta_p.name

                        # Derive binary: meta filename without ".json" suffix
                        binary_p = meta_p.with_suffix("")  # removes only .json

                        # Build rel paths regardless of validity (must be NOT NULL in DB)
                        meta_rel_path = f"ide/{project}/{ver}/{platform}/{meta_name}"
                        binary_rel_path = f"ide/{project}/{ver}/{platform}/{binary_p.name}"

                        # published_ts = max(mtime(meta), mtime(binary)) when possible
                        try:
                            mt = int(meta_p.stat().st_mtime)
                        except Exception:
                            mt = now
                        bt = mt
                        if binary_p.is_file():
                            try:
                                bt = int(binary_p.stat().st_mtime)
                            except Exception:
                                bt = mt
                        published_ts = max(mt, bt)

                        if not binary_p.is_file():
                            invalid_reason = "binary_missing"
                        else:
                            obj = _read_json_utf8(meta_p)
                            if obj is None:
                                invalid_reason = "meta_json_unparseable"
                            else:
                                sub_product_name = _req_str(obj, "sub_product_name")
                                version = _req_str(obj, "version")
                                os_type = _req_str(obj, "os_type")
                                arch = _req_str(obj, "arch")

                                if not (sub_product_name and version and os_type and arch):
                                    invalid_reason = "meta_missing_required_fields"
                                else:
                                    if sub_product_name != project:
                                        invalid_reason = "meta_project_mismatch"
                                    elif version != ver:
                                        invalid_reason = "meta_version_mismatch"
                                    else:
                                        try:
                                            norm_plat = normalize_platform(os_type, arch)
                                        except Exception:
                                            norm_plat = ""
                                        if not norm_plat:
                                            invalid_reason = "platform_normalize_failed"
                                        elif norm_plat != platform:
                                            invalid_reason = "platform_dir_mismatch"
                                        else:
                                            is_valid = 1
                                            invalid_reason = None

                    # If meta wasn't present (or multiple), still store deterministic relpaths
                    if not meta_rel_path:
                        meta_rel_path = f"ide/{project}/{ver}/{platform}/"
                    if not binary_rel_path:
                        binary_rel_path = f"ide/{project}/{ver}/{platform}/"

                    out.append(
                        IdePlatformRow(
                            project=project,
                            version=ver,
                            platform=platform,
                            meta_rel_path=meta_rel_path,
                            binary_rel_path=binary_rel_path,
                            published_ts=int(published_ts),
                            is_latest=int(is_latest),
                            is_valid=int(is_valid),
                            invalid_reason=invalid_reason,
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
        """
        List versions that have at least one valid platform artifact.
        published_ts = max published_ts among valid platforms within version.
        is_latest computed from indexed rows (set during rebuild).
        """
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
            FROM ide_platforms
            WHERE project=? AND is_valid=1
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
        Returns (binary_rel_path, binary_filename) for stable latest of project for the exact platform.

        Per TZ:
        - NO universal fallback
        - platform must match normalize_platform(os_type, arch) == platform dir name (enforced in is_valid=1)
        - choose only is_latest=1 AND is_valid=1
        """
        self._ensure_inited()
        proj = (project or "").strip()
        plat = (platform or "").strip()
        if not _safe_seg(proj) or not _safe_seg(plat):
            return None

        conn = self._conn()
        row = conn.execute(
            """
            SELECT binary_rel_path
            FROM ide_platforms
            WHERE project=? AND platform=? AND is_latest=1 AND is_valid=1
            LIMIT 1
            """,
            (proj, plat),
        ).fetchone()
        if not row:
            return None

        rel = str(row["binary_rel_path"])
        name = Path(rel).name
        return (rel, name)


IDE_REGISTRY = IdeRegistry()
