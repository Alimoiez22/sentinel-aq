"""Every log line is one JSON object with timestamp, level and event."""

from __future__ import annotations

import io
import json
from datetime import datetime

from sentinel.log import configure_logging, get_logger


def test_json_line_with_required_keys() -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)
    get_logger(batch_id="b-1").info("loaded", rows=48)

    record = json.loads(stream.getvalue().strip())
    assert record["event"] == "loaded"
    assert record["level"] == "info"
    assert record["batch_id"] == "b-1"
    assert record["rows"] == 48
    assert datetime.fromisoformat(record["timestamp"]).utcoffset() is not None


def test_level_filter_drops_debug() -> None:
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)
    get_logger().debug("noise")
    assert stream.getvalue() == ""
