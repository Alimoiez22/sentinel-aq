"""Crash points the fault injector can arm (F10, F11, F12). Inert unless armed.

A crash point is a named place in the pipeline. Arming one makes the NEXT pass through
it fail in a controlled way, exactly once (a real crash does not repeat by itself):
* "raise": raise InjectedCrash (a process dying mid-transaction; the transaction rolls back)
* "drop_connection": terminate this session's database backend (F12), so the next
  statement fails with an OperationalError, as a real network drop would.
"""

from __future__ import annotations

from typing import Any

_armed: dict[str, str] = {}


class InjectedCrash(RuntimeError):
    """Raised by an armed crash point."""


def arm(point: str, how: str = "raise") -> None:
    """Make the next pass through `point` fail. `how` is 'raise' or 'drop_connection'."""
    if how not in {"raise", "drop_connection"}:
        raise ValueError(how)
    _armed[point] = how


def disarm_all() -> None:
    """Remove every armed crash point."""
    _armed.clear()


def armed() -> dict[str, str]:
    """Currently armed points (for tests and evidence)."""
    return dict(_armed)


def crashpoint(point: str, conn: Any = None) -> None:
    """Fail here if armed; otherwise do nothing."""
    how = _armed.pop(point, None)
    if how is None:
        return
    if how == "drop_connection" and conn is not None:
        import psycopg

        params = conn.info.get_parameters()  # host, port, dbname, user (no password)
        with psycopg.connect(_conninfo(params["dbname"]), autocommit=True) as killer:
            killer.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
        conn.execute("SELECT 1")  # raises OperationalError: the server closed the connection
        return
    raise InjectedCrash(f"injected crash at {point}")


def _conninfo(dbname: str) -> str:
    from sentinel.settings import get_settings

    return get_settings().conninfo(dbname=dbname)
