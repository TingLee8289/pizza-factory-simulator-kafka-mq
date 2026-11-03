"""Environment-driven settings shared by every service."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Broker connection settings (env: KAFKA_BOOTSTRAP, RABBITMQ_URL)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap: str = "127.0.0.1:9094"
    rabbitmq_url: str = "amqp://pizza:pizza@127.0.0.1:5672/"


@lru_cache
def get_settings() -> Settings:
    """Return the cached process-wide settings."""
    return Settings()
