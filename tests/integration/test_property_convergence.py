"""Property (M3/M7): any order of arrivals, duplicates, replays and revisions converges to
the golden state built from the last NEW version of each location-day."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from sentinel.openaq.archive import parse_key
from sentinel.ops import load_guidelines, reset_database, seed_locations
from sentinel.pipeline import Pipeline
from sentinel.quality import load_quality_config
from sentinel.rawstore import RawStore
from sentinel.reconcile import compare_all, rebuild_golden
from tests.integration.conftest import NOW, ROOT, SYNTH, connect_test, synthetic_files
from tests.integration.test_pipeline import T, edit_values

pytestmark = pytest.mark.integration

FILES = dict(synthetic_files()[:2])  # location A, Nov 1 and Nov 2
KEYS = sorted(FILES)
VERSIONS = {k: [FILES[k], edit_values(FILES[k], 1.5, slice(0, 4)),
                edit_values(FILES[k], 0.5, slice(10, 12))] for k in KEYS}  # fmt: skip

ops = st.lists(
    st.one_of(
        st.tuples(st.just("arrive"), st.sampled_from(KEYS), st.integers(0, 2)),
        st.tuples(st.just("replay"), st.sampled_from(KEYS), st.just(0)),
    ),
    min_size=1, max_size=8,
)  # fmt: skip


@settings(
    max_examples=12, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(sequence=ops)
def test_any_sequence_converges_to_golden(migrated: None, tmp_path: Path, sequence: list) -> None:  # type: ignore[type-arg]
    pipe = Pipeline(
        RawStore(tmp_path / "raw"),
        load_quality_config(ROOT / "config/quality_rules.yaml"),
        ("pm25", "pm10"),
        now=lambda: NOW,
    )
    with connect_test() as conn:
        reset_database(conn)
        load_guidelines(conn, ROOT / "config/guidelines.yaml")
        seed_locations(conn, SYNTH / "LOCATIONS.csv")
        seen: dict[str, set[bytes]] = {k: set() for k in KEYS}
        latest: dict[str, bytes] = {}
        for op, key, v in sequence:
            loc, d = parse_key(key)
            if op == "arrive":
                data = VERSIONS[key][v]
                pipe.register(conn, key, data, T)
                if data not in seen[key]:  # an old version arriving again is a duplicate
                    seen[key].add(data)
                    latest[key] = data
                bid = pipe.current_batch(conn, loc, d)
                assert bid is not None
                assert pipe.process(conn, bid)
            else:
                bid = pipe.current_batch(conn, loc, d)
                if bid is None:
                    continue
                pipe.stage(conn, bid)  # replay every stage, ignoring checkpoints
                pipe.quality_and_fact(conn, bid)
                pipe.marts_and_gate(conn, bid)
                assert pipe.verify(conn, bid)
        rebuild_golden(conn, pipe, {parse_key(k): b for k, b in latest.items()})
        diffs = compare_all(conn)
        assert all(x.equal and x.checksum_equal for x in diffs), [x for x in diffs if not x.equal]


def test_dates_used() -> None:
    assert [parse_key(k)[1] for k in KEYS] == [date(2025, 11, 1), date(2025, 11, 2)]
