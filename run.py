from __future__ import annotations

import os

from core.app import create_app
from core.config import AppConfig, setup_logging, _env_bool


def main() -> None:
    cfg = AppConfig.from_env()
    setup_logging(cfg)

    debug = _env_bool("DEBUG", False)
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    app = create_app(cfg)
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
