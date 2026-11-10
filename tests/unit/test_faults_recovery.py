"""Fault catalogue is deterministic and manifests; classifier and policy are consistent."""

from __future__ import annotations

import gzip
from datetime import date
from pathlib import Path

import pytest

from sentinel.contract import ContractError, parse_file
from sentinel.faults import EXPECTED, FAULTS, build_trial
from sentinel.recovery import Evidence, classify, load_policy

ROOT = Path(__file__).resolve().parents[2]
SYNTH = ROOT / "data/fixtures/synthetic"
CLEAN = {p.relative_to(SYNTH).as_posix(): p.read_bytes() for p in sorted(SYNTH.rglob("*.csv.gz"))}
D = date(2025, 11, 1)


@pytest.mark.parametrize("fault", FAULTS)
def test_trials_are_deterministic(fault: str) -> None:
    a, b = build_trial(fault, 7, CLEAN), build_trial(fault, 7, CLEAN)
    assert (a.target, a.deliveries, a.truth, a.crash) == (b.target, b.deliveries, b.truth, b.crash)


def _target_bytes(fault: str) -> tuple[bytes, bytes]:
    t = build_trial(fault, 3, CLEAN)
    return CLEAN[t.target], t.deliveries[0].get(t.target, b"")


def test_each_file_fault_manifests() -> None:
    """Each fault really changes what arrives (a fault that does nothing tests nothing)."""
    _, f2 = _target_bytes("F2")
    with pytest.raises(ContractError, match="unreadable"):
        parse_file(f2)
    _, f5 = _target_bytes("F5")
    with pytest.raises(ContractError, match="unknown_header"):
        parse_file(f5)
    clean4, f4 = _target_bytes("F4")
    assert parse_file(f4).variant != parse_file(clean4).variant  # the other KNOWN variant
    for fault, needle in (("F3", b"#ERR"), ("F13", b"ppm"), ("F15", b"+00:00")):
        _, data = _target_bytes(fault)
        assert needle in gzip.decompress(data), fault
    _, f6 = _target_bytes("F6")
    assert parse_file(f6).frame["value"].null_count() == parse_file(f6).row_count
    t7, t8 = build_trial("F7", 3, CLEAN), build_trial("F8", 3, CLEAN)
    assert t7.target not in t7.deliveries[0] and t7.target in t7.source_at[0]
    assert t8.target not in t8.source_at[0] and t8.target in t8.deliveries[1]
    t9 = build_trial("F9", 3, CLEAN)
    assert t9.deliveries[1][t9.target] != CLEAN[t9.target]
    assert build_trial("F10", 3, CLEAN).crash == (0, "after_staging_before_commit", "raise")
    assert build_trial("F12", 3, CLEAN).crash == (
        0,
        "after_staging_before_commit",
        "drop_connection",
    )


def test_truth_follows_where_the_fault_happens() -> None:
    """Transfer faults: truth is the clean file. Upstream data faults: truth is the faulty file."""
    from sentinel.openaq.archive import parse_key

    for fault in ("F2", "F5", "F7"):
        t = build_trial(fault, 4, CLEAN)
        assert t.truth[parse_key(t.target)] == CLEAN[t.target]
    for fault in ("F3", "F6", "F13", "F15"):
        t = build_trial(fault, 4, CLEAN)
        assert t.truth[parse_key(t.target)] == t.source_at[0][t.target] != CLEAN[t.target]


@pytest.mark.parametrize(
    ("ev", "expected"),
    [
        (Evidence("run_error", 1, D, 1, {"error": "AdminShutdown", "transient": True}),
         "transient_db"),
        (Evidence("run_error", 1, D, 1, {"error": "InjectedCrash", "transient": False,
                                         "stages_done": ["staged"]}), "process_crash"),
        (Evidence("rejected_file", 1, D, 1, {"reason": "unreadable: x", "landing_sha256": "a",
                                             "source_sha256": "b"}), "transfer_corruption"),
        (Evidence("rejected_file", 1, D, 1, {"reason": "unreadable: x", "landing_sha256": "a",
                                             "source_sha256": "a"}), "source_unreadable"),
        (Evidence("rejected_file", 1, D, 1, {"reason": "unknown_header: a,b"}), "unknown_schema"),
        (Evidence("no_file", 1, D, None, {"in_source": True}), "missing_file"),
        (Evidence("no_file", 1, D, None, {"in_source": False, "hours_since_day_end": 50}),
         "late_file"),
        (Evidence("no_file", 1, D, None, {"in_source": False, "hours_since_day_end": 500}),
         "unknown"),
        (Evidence("blocked", 1, D, 1, {"counts": {"Q05_unknown_unit": 30, "Q02_bad_value": 2}}),
         "unknown_unit"),
        (Evidence("blocked", 1, D, 1, {"counts": {"Q08_offset_mismatch": 48}}), "timezone_offset"),
        (Evidence("blocked", 1, D, 1, {"counts": {"Q02_bad_value": 20, "Q14_spike_hampel": 40}}),
         "rows_over_threshold"),
        (Evidence("not_verified", 1, D, 1, {}), "verification_failed"),
        (Evidence("revision", 1, D, 1, {}), "revision"),
        (Evidence("something_new", 1, D, 1, {}), "unknown"),
    ],
)  # fmt: skip
def test_classifier(ev: Evidence, expected: str) -> None:
    assert classify(ev) == expected


def test_policy_covers_every_class_and_allow_lists_actions() -> None:
    policy = load_policy(ROOT / "config/policy.yaml")
    classes = {e.failure_class for e in EXPECTED.values() if e.failure_class}
    for c in classes | {"unknown", "verification_failed", "source_unreadable"}:
        assert policy.for_class(c).failure_class == c
    assert policy.for_class("never-seen").id == "P99"
    for c in ("unknown_schema", "unknown_unit", "timezone_offset"):
        assert policy.for_class(c).action == "escalate"  # never automatic (brief §12)
    for e in EXPECTED.values():
        if e.failure_class and not e.automatic:
            assert policy.for_class(e.failure_class).action == "escalate"
