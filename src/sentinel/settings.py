"""Runtime settings, read from environment variables (and `.env` when present).

Why pydantic-settings: a missing or malformed setting fails at start-up with a clear
message, instead of failing later inside a query with a confusing one.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Connection and logging settings for the warehouse database."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", frozen=True)

    pg_host: str = Field(default="localhost", alias="PGHOST")
    pg_port: int = Field(default=5434, alias="PGPORT", ge=1, le=65535)
    pg_user: str = Field(alias="SENTINEL_DB_USER", min_length=1)
    pg_password: SecretStr = Field(alias="SENTINEL_DB_PASSWORD")
    pg_database: str = Field(alias="SENTINEL_DB_NAME", min_length=1)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", alias="SENTINEL_LOG_LEVEL"
    )

    def conninfo(self) -> str:
        """Return a libpq connection string. The password is never logged."""
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_database} "
            f"user={self.pg_user} password={self.pg_password.get_secret_value()}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load settings once per process."""
    return Settings()
