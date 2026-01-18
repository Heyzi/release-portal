from __future__ import annotations

import json
import logging
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from flask import Flask, Response, jsonify, request, g
from werkzeug.middleware.proxy_fix import ProxyFix

from core.config import AppConfig

from services import releases as releases_service
from services.extensions_registry import REGISTRY


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _truncate(v: Any, max_len: int) -> Any:
    if v is None:
        return None
    try:
        s = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False, default=str)
    except Exception:
        s = str(v)
    if len(s) <= max_len:
        return v
    return s[:max_len] + "…"


def _safe_headers() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in request.headers.items():
        lk = k.lower()
        if lk in {"authorization", "cookie", "set-cookie"}:
            continue
        out[k] = v
    return out


def _register_blueprints(app: Flask) -> None:
    from api.ide import bp_ide
    from api.extensions_marketplace import bp_marketplace
    from api.releases_api import bp_releases
    from api.portal import bp_portal

    app.register_blueprint(bp_ide, url_prefix="")
    app.register_blueprint(bp_marketplace, url_prefix="")
    app.register_blueprint(bp_releases, url_prefix="")
    app.register_blueprint(bp_portal, url_prefix="")


def _apply_common_headers(resp: Response) -> Response:
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers["X-Request-Id"] = getattr(g, "request_id", "")
    return resp


def create_app(cfg: AppConfig) -> Flask:
    base_dir = Path(__file__).resolve().parents[1]
    templates_dir = base_dir / "templates"
    static_dir = base_dir / "static"

    app = Flask(
        __name__,
        template_folder=str(templates_dir) if templates_dir.is_dir() else None,
        static_folder=str(static_dir) if static_dir.is_dir() else None,
    )

    releases_service.set_releases_root(cfg.releases_root)

    try:
        REGISTRY.init_and_rebuild()
    except Exception:
        logging.getLogger(__name__).exception("extensions_registry_init_failed")

    # Reverse proxy support (common: proto/host/for)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)  # type: ignore[method-assign]

    @app.before_request
    def _before_request() -> None:
        g.request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex
        g._t0 = time.perf_counter()

    @app.after_request
    def _after_request(resp: Response) -> Response:
        try:
            dur_ms = int((time.perf_counter() - getattr(g, "_t0", time.perf_counter())) * 1000)
        except Exception:
            dur_ms = None

        rec = logging.LogRecord(
            name="access",
            level=logging.INFO,
            pathname=__file__,
            lineno=0,
            msg="request",
            args=(),
            exc_info=None,
        )
        rec.request_id = getattr(g, "request_id", None)
        rec.remote_addr = request.headers.get("X-Forwarded-For", request.remote_addr)
        rec.method = request.method
        rec.path = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
        rec.status = resp.status_code
        rec.duration_ms = dur_ms
        rec.bytes = resp.calculate_content_length()
        rec.ref = request.headers.get("Referer")
        rec.ua = request.headers.get("User-Agent")
        rec.host = request.host

        if app.debug:
            rec.extra = {"query": request.args.to_dict(flat=True), "headers": _safe_headers()}

        logging.getLogger("access").handle(rec)
        return _apply_common_headers(resp)

    @app.errorhandler(Exception)
    def _handle_exception(e: Exception):
        status = getattr(e, "code", 500)

        rec = logging.LogRecord(
            name="error",
            level=logging.ERROR if int(status) >= 500 else logging.WARNING,
            pathname=__file__,
            lineno=0,
            msg="exception",
            args=(),
            exc_info=sys.exc_info(),
        )
        rec.request_id = getattr(g, "request_id", None)
        rec.remote_addr = request.headers.get("X-Forwarded-For", request.remote_addr)
        rec.method = request.method
        rec.path = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
        rec.status = int(status)

        if app.debug:
            try:
                body_json = request.get_json(silent=True)
            except Exception:
                body_json = None
            rec.extra = {
                "query": request.args.to_dict(flat=True),
                "headers": _safe_headers(),
                "form": _truncate(request.form.to_dict(flat=True), 5000),
                "json": _truncate(body_json, 20000),
            }

        logging.getLogger("error").handle(rec)

        payload: Dict[str, Any] = {
            "error": True,
            "status": int(status),
            "message": str(e),
            "requestId": getattr(g, "request_id", None),
            "timestamp": _utc_iso(),
        }
        if app.debug:
            payload["traceback"] = _truncate("".join(traceback.format_exception(*sys.exc_info())), 20000)

        resp = jsonify(payload)
        resp.status_code = int(status)
        return _apply_common_headers(resp)

    @app.get("/health")
    def health():
        return jsonify({"ok": True, "timestamp": _utc_iso()}), 200

    _register_blueprints(app)

    logging.getLogger(__name__).info("app_started releases_root=%s", str(cfg.releases_root))
    return app
