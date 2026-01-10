# logging_setup.py
from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__(datefmt="%Y-%m-%dT%H:%M:%S%z")

    def format(self, record: logging.LogRecord) -> str:
        obj: Dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.levelno >= logging.WARNING:
            obj["filename"] = record.filename
            obj["funcName"] = record.funcName
            obj["line"] = record.lineno
        if record.exc_info:
            obj["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(obj, ensure_ascii=False)


def configure_logging() -> logging.Logger:
    """
    Configure root logger and Werkzeug logger to emit JSON logs.
    Returns the application logger instance.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(JsonFormatter())
    root_logger.addHandler(stream)

    werkzeug_logger = logging.getLogger("werkzeug")
    werkzeug_logger.handlers.clear()
    werkzeug_logger.propagate = True
    werkzeug_logger.setLevel(logging.WARNING)

    return logging.getLogger("release_portal")
