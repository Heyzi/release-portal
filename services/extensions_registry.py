from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from services import releases as releases_service


ALLOWED_PLATFORMS = {
    "win32-x64",
    "win32-ia32",
    "win32-arm64",
    "linux-x64",
    "linux-arm64",
    "linux-armhf",
    "alpine-x64",
    "alpine-arm64",
    "darwin-x64",
    "darwin-arm64",
    "web",
    "universal",
}


def _ext_root() -> Path:
    # Extensions root directory
    return Path(releases_service.RELEASES_ROOT) / "extensions"


def _db_path() -> Path:
    # SQLite index path
    return Path(releases_service.RELEASES_ROOT) / "_indexes" / "extensions_index.sqlite"


def _normalize_tp(tp: Optional[str]) -> str:
    # Normalize target platform
    v = (tp or "").strip().lower()
    if not v or v == "universal":
        return "universal"
    if v not in ALLOWED_PLATFORMS:
        return "universal"
    return v


_SORT_RE = __import__("re").compile(r"(\d+|[A-Za-z]+)")


def version_key(v: str) -> Tuple[int, ...]:
    # Stable sortable key that keeps numeric parts numeric
    parts = _SORT_RE.findall(v or "")
    out: List[int] = []
    for p in parts:
        if p.isdigit():
            out.append(10_000_000)
            out.append(int(p))
        else:
            out.append(1)
            out.extend(ord(c) for c in p.lower())
    out.append(0)
    return tuple(out)


@dataclass(frozen=True)
class ExtRow:
    # Extension record row
    namespace: str
    name: str
    version: str
    target_platform: str
    dir_path: Path
    published_ts: int

    @property
    def vsix_path(self) -> Path:
        # VSIX path
        return self.dir_path / "extension.vsix"

    @property
    def unpacked_dir(self) -> Path:
        # Unpacked dir path
        return self.dir_path / "unpacked"


class ExtensionsRegistry:
    # SQLite-backed registry
    def __init__(self) -> None:
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self._inited = False

    def _conn(self) -> sqlite3.Connection:
        # Thread-local connection
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
                    CREATE TABLE IF NOT EXISTS extensions (
                        namespace TEXT NOT NULL,
                        name TEXT NOT NULL,
                        version TEXT NOT NULL,
                        target_platform TEXT NOT NULL,
                        dir_path TEXT NOT NULL,
                        published_ts INTEGER NOT NULL,
                        PRIMARY KEY (namespace, name, version, target_platform)
                    )
                    """
                )
                conn.execute("DELETE FROM extensions")
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
                        INSERT INTO extensions(namespace, name, version, target_platform, dir_path, published_ts)
                        VALUES(?, ?, ?, ?, ?, ?)
                        """,
                        [(r.namespace, r.name, r.version, r.target_platform, str(r.dir_path), r.published_ts) for r in rows],
                    )
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

            conn.execute("CREATE INDEX IF NOT EXISTS idx_ext_ns_name ON extensions(namespace, name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ext_ns_name_tp ON extensions(namespace, name, target_platform)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ext_ns_name_ver ON extensions(namespace, name, version)")

            self._inited = True

    def _scan_fs_rows(self) -> List[ExtRow]:
        # Scan filesystem for vsix files
        root = _ext_root()
        if not root.exists():
            return []
        out: List[ExtRow] = []
        now = int(time.time())

        for ns_dir in root.iterdir():
            if not ns_dir.is_dir():
                continue
            ns = ns_dir.name.strip().lower()
            if not ns:
                continue

            for ext_dir in ns_dir.iterdir():
                if not ext_dir.is_dir():
                    continue
                name = ext_dir.name.strip().lower()
                if not name:
                    continue

                for ver_dir in ext_dir.iterdir():
                    if not ver_dir.is_dir():
                        continue
                    ver = ver_dir.name.strip()
                    if not ver:
                        continue

                    for tp_dir in ver_dir.iterdir():
                        if not tp_dir.is_dir():
                            continue
                        tp = _normalize_tp(tp_dir.name)
                        if tp_dir.name.strip().lower() not in ALLOWED_PLATFORMS:
                            continue
                        if (tp_dir / "extension.vsix").is_file():
                            try:
                                ts = int((tp_dir / "extension.vsix").stat().st_mtime)
                            except Exception:
                                ts = now
                            out.append(ExtRow(ns, name, ver, tp, tp_dir, ts))

        return out

    def upsert(
        self,
        namespace: str,
        name: str,
        version: str,
        target_platform: str,
        dir_path: Path,
        published_ts: Optional[int] = None,
    ) -> None:
        # Insert or update a record
        if not self._inited:
            self.init_and_rebuild()

        ns = (namespace or "").strip().lower()
        nm = (name or "").strip().lower()
        ver = (version or "").strip()
        tp = _normalize_tp(target_platform)
        if not published_ts:
            published_ts = int(time.time())

        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                """
                INSERT INTO extensions(namespace, name, version, target_platform, dir_path, published_ts)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(namespace, name, version, target_platform)
                DO UPDATE SET dir_path=excluded.dir_path, published_ts=excluded.published_ts
                """,
                (ns, nm, ver, tp, str(dir_path), int(published_ts)),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def list_pairs(self, search_text: Optional[str] = None) -> List[Tuple[str, str]]:
        # List (namespace, name) pairs
        if not self._inited:
            self.init_and_rebuild()
        conn = self._conn()
        st = (search_text or "").strip().lower()
        if st:
            rows = conn.execute(
                """
                SELECT DISTINCT namespace, name
                FROM extensions
                WHERE (namespace || '.' || name) LIKE ?
                ORDER BY namespace, name
                """,
                (f"%{st}%",),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT DISTINCT namespace, name
                FROM extensions
                ORDER BY namespace, name
                """
            ).fetchall()
        return [(str(r["namespace"]), str(r["name"])) for r in rows]

    def list_records(self, namespace: str, name: str) -> List[ExtRow]:
        # List all records for an extension
        if not self._inited:
            self.init_and_rebuild()
        ns = (namespace or "").strip().lower()
        nm = (name or "").strip().lower()
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT namespace, name, version, target_platform, dir_path, published_ts
            FROM extensions
            WHERE namespace=? AND name=?
            """,
            (ns, nm),
        ).fetchall()

        out: List[ExtRow] = []
        for r in rows:
            out.append(
                ExtRow(
                    namespace=str(r["namespace"]),
                    name=str(r["name"]),
                    version=str(r["version"]),
                    target_platform=str(r["target_platform"]),
                    dir_path=Path(str(r["dir_path"])),
                    published_ts=int(r["published_ts"]),
                )
            )
        out.sort(key=lambda x: version_key(x.version), reverse=True)
        return out

    def pick_record(self, namespace: str, name: str, version: str, tp_req: Optional[str]) -> Optional[ExtRow]:
        # Pick a single record matching version and platform preference
        recs = self.list_records(namespace, name)
        if not recs:
            return None

        ver = (version or "").strip()
        want_tp = _normalize_tp(tp_req)

        candidates = [r for r in recs if r.version == ver]
        if not candidates:
            return None

        if tp_req and want_tp != "universal":
            for r in candidates:
                if r.target_platform == want_tp:
                    return r
            for r in candidates:
                if r.target_platform == "universal":
                    return r
            return candidates[0]

        for r in candidates:
            if r.target_platform == "universal":
                return r
        return candidates[0]

    def latest_for(self, namespace: str, name: str, tp_req: Optional[str]) -> Optional[List[ExtRow]]:
        # Return records sorted by version with optional platform filtering
        recs = self.list_records(namespace, name)
        if not recs:
            return None

        want_tp = _normalize_tp(tp_req)
        if tp_req and want_tp != "universal":
            filtered = [r for r in recs if r.target_platform in {want_tp, "universal"}]
            if filtered:
                filtered.sort(key=lambda x: version_key(x.version), reverse=True)
                return filtered
        return recs


REGISTRY = ExtensionsRegistry()
