from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


# =========================
# env helpers
# =========================

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip()


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on"}


# =========================
# config
# =========================

@dataclass(frozen=True)
class AppConfig:
    releases_root: Path
    log_level: str
    json_logs: bool

    @staticmethod
    def from_env() -> "AppConfig":
        # raw values from env
        releases_root_raw = _env_str("RELEASES_ROOT", "./data/releases")
        log_level = _env_str("LOG_LEVEL", "INFO").upper()
        json_logs = _env_bool("JSON_LOGS", False)

        project_root = Path(__file__).resolve().parents[1]

        p = Path(releases_root_raw).expanduser()

        if not p.is_absolute():
            p = (project_root / p).resolve()

        return AppConfig(
            releases_root=p,
            log_level=log_level,
            json_logs=json_logs,
        )


# =========================
# logging
# =========================

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(cfg: AppConfig) -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, cfg.log_level, logging.INFO))

    # reset handlers (idempotent setup)
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    if cfg.json_logs:
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )

    root.addHandler(handler)

    # sane defaults for noisy libs
    logging.getLogger("werkzeug").setLevel(logging.INFO)
