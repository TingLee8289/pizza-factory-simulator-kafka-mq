"""Entrypoint: python -m pizza.kpi."""

import asyncio
import signal
from urllib.parse import urlparse

import structlog

from pizza.common.kafka_io import make_producer
from pizza.common.rabbit_io import connect
from pizza.common.recipes import load_all_recipes, validate_recipes
from pizza.kpi.service import Kpi
from pizza.kpi.settings import get_kpi_settings

_log = structlog.get_logger()


async def _amain() -> None:
    settings = get_kpi_settings()
    # Never log the full URL (credentials); host only.
    _log.info("kpi_boot", rabbit_host=urlparse(settings.rabbitmq_url).hostname)
    recipes = load_all_recipes(settings.recipes_dir)
    validate_recipes(recipes)
    producer = make_producer(settings.kafka_bootstrap)
    connection = await connect(settings.rabbitmq_url)
    kpi = Kpi(producer, settings, recipes)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, kpi.stop)
        except NotImplementedError:
            # Windows host: asyncio has no add_signal_handler.
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(kpi.stop))

    try:
        await kpi.run(connection)
    finally:
        await connection.close()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
