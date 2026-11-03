"""Entrypoint: python -m pizza.generator."""

import asyncio
import signal
from urllib.parse import urlparse

import structlog

from pizza.common.kafka_io import make_producer
from pizza.common.rabbit_io import connect
from pizza.generator.service import Generator
from pizza.generator.settings import get_generator_settings

_log = structlog.get_logger()


async def _amain() -> None:
    settings = get_generator_settings()
    # Never log the full URL (credentials); host only.
    _log.info("generator_boot", rabbit_host=urlparse(settings.rabbitmq_url).hostname)
    producer = make_producer(settings.kafka_bootstrap)
    connection = await connect(settings.rabbitmq_url)
    generator = Generator(producer, settings)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, generator.stop)
        except NotImplementedError:
            # Windows host: asyncio has no add_signal_handler.
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(generator.stop))

    try:
        await generator.run(connection)
    finally:
        await connection.close()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
