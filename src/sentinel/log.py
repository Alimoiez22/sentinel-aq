"""Structured JSON logging (rule 11).

Every log line is one JSON object with at least `timestamp` (UTC, ISO-8601), `level`
and `event`. Later milestones add `batch_id`, `location_id` and `source_date` as bound
fields, so a single grep over the logs reconstructs what happened to one location-day.
"""

from __future__ import annotations

import logging
import sys
from typing import Any, TextIO

import structlog


def configure_logging(level: str = "INFO", stream: TextIO | None = None) -> None:
    """Send JSON log lines to `stream` (stderr by default) at `level` and above."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(sort_keys=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        logger_factory=structlog.PrintLoggerFactory(file=stream or sys.stderr),
        cache_logger_on_first_use=False,
    )


def get_logger(**initial: Any) -> structlog.typing.FilteringBoundLogger:
    """Return a logger with `initial` fields bound to every line it writes."""
    logger: structlog.typing.FilteringBoundLogger = structlog.get_logger().bind(**initial)
    return logger
