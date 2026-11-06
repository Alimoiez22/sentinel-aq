"""Settings must fail early and never leak the password."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from sentinel.settings import Settings

BASE_ENV = {
    "SENTINEL_DB_USER": "sentinel",
    "SENTINEL_DB_PASSWORD": "s3cret-value",
    "SENTINEL_DB_NAME": "warehouse",
    "PGHOST": "db",
    "PGPORT": "5432",
}


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.chdir("/")  # make sure no developer .env is picked up
    return monkeypatch


def test_reads_environment(env: pytest.MonkeyPatch) -> None:
    s = Settings()
    assert (s.pg_host, s.pg_port, s.pg_database) == ("db", 5432, "warehouse")
    assert "dbname=warehouse" in s.conninfo()


def test_password_is_hidden_in_repr(env: pytest.MonkeyPatch) -> None:
    s = Settings()
    assert "s3cret-value" not in repr(s)
    assert "s3cret-value" not in str(s.model_dump())


def test_missing_user_fails(env: pytest.MonkeyPatch) -> None:
    env.delenv("SENTINEL_DB_USER")
    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("port", ["0", "70000", "abc"])
def test_bad_port_fails(env: pytest.MonkeyPatch, port: str) -> None:
    env.setenv("PGPORT", port)
    with pytest.raises(ValidationError):
        Settings()


def test_bad_log_level_fails(env: pytest.MonkeyPatch) -> None:
    env.setenv("SENTINEL_LOG_LEVEL", "LOUD")
    with pytest.raises(ValidationError):
        Settings()
