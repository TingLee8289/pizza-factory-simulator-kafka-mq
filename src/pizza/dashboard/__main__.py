"""Entrypoint: python -m pizza.dashboard."""

from urllib.parse import urlparse

import structlog
import uvicorn

from pizza.dashboard.settings import get_dashboard_settings

_log = structlog.get_logger()


def main() -> None:
    settings = get_dashboard_settings()
    # Never log the full URL (credentials); host only.
    _log.info(
        "dashboard_boot",
        rabbit_host=urlparse(settings.rabbitmq_url).hostname,
        port=settings.dashboard_port,
    )
    uvicorn.run(
        "pizza.dashboard.app:create_app",
        factory=True,
        host=settings.dashboard_host,
        port=settings.dashboard_port,
        workers=1,  # in-memory state is single-process
        timeout_graceful_shutdown=2,  # hung SSE connections must not stall `compose down`
    )


if __name__ == "__main__":
    main()
