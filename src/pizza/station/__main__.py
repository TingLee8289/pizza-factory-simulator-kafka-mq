"""Entrypoint: python -m pizza.station (STATION_NAME selects the station)."""

import asyncio
import signal
from urllib.parse import urlparse

import structlog

from pizza.common.kafka_io import make_producer
from pizza.common.rabbit_io import connect
from pizza.station.service import Station
from pizza.station.settings import get_station_settings

_log = structlog.get_logger()


async def _amain() -> None:
    settings = get_station_settings()
    # Never log the full URL (credentials); host only.
    _log.info(
        "station_boot",
        station=settings.station_name,
        rabbit_host=urlparse(settings.rabbitmq_url).hostname,
    )
    producer = make_producer(settings.kafka_bootstrap)
    connection = await connect(settings.rabbitmq_url)
    station = Station(settings.station_name, producer, settings)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, station.stop)
        except NotImplementedError:
            # Windows host: asyncio has no add_signal_handler.
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(station.stop))

    try:
        await station.run(connection)
    finally:
        await connection.close()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
