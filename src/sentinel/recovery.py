"""Recovery loop (M10): Detect -> Evidence -> Classify -> Policy -> ONE action or escalate
-> Verify -> Record. Deterministic: the classifier is a fixed function of the evidence
(version clf-1), and every decision records the policy rule that made it.

Modes compared in the experiments (M11):
* guarded       - the full loop above
* retries_only  - rerun the whole run on any run error, at most 3 times, nothing else
* manual        - the pipeline halts; a person follows docs/RUNBOOK.md (not automated)
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Protocol

import psycopg
import yaml
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field

from sentinel.ops import RunSummary, run_landing
from sentinel.pipeline import Conn, Pipeline
from sentinel.rawstore import sha256_hex, write_atomic

CLASSIFIER_VERSION = "clf-1"
TRANSIENT_ERRORS = {"OperationalError", "InterfaceError", "ConnectionError", "TimeoutError"}
GRACE_HOURS_AFTER_DAY_END = 96  # 72 h publication delay (RESEARCH §5.1) + 24 h margin


class PolicyRule(BaseModel):
    """One row of config/policy.yaml."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str
    failure_class: str
    action: str
    max_attempts: int = Field(ge=0, le=5)
    backoff_s: float = Field(ge=0)
    risk: Literal["low", "medium", "high"]
    verify: bool
    runbook: str
    escalate: bool = False
    max_days: int | None = None


class Policy(BaseModel):
    """Validated config/policy.yaml."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    version: int
    classifier_version: str
    allowed_actions: list[str]
    never_automatic: list[str]
    rules: list[PolicyRule]

    def for_class(self, failure_class: str) -> PolicyRule:
        for r in self.rules:
            if r.failure_class == failure_class:
                return r
        return self.for_class("unknown")


def load_policy(path: Path = Path("config/policy.yaml")) -> Policy:
    """Read and validate the policy; every non-escalate action must be allow-listed."""
    with path.open(encoding="utf-8") as f:
        policy = Policy.model_validate(yaml.safe_load(f))
    for r in policy.rules:
        if r.action != "escalate" and r.action not in policy.allowed_actions:
            raise ValueError(f"{r.id}: action {r.action} is not allow-listed")
    return policy


class Source(Protocol):
    """Where a file can be fetched again (the upstream archive)."""

    def get(self, key: str) -> bytes | None: ...


@dataclass
class DictSource:
    """An in-memory upstream archive (experiments)."""

    files: dict[str, bytes]

    def get(self, key: str) -> bytes | None:
        return self.files.get(key)


@dataclass(frozen=True)
class Evidence:
    """One observed problem for a location-day."""

    kind: str  # run_error | rejected_file | no_file | blocked | not_verified | revision
    location_id: int
    source_date: date
    batch_id: int | None
    data: dict[str, Any]

    @property
    def key(self) -> tuple[str, int, date, int | None]:
        return (self.kind, self.location_id, self.source_date, self.batch_id)


def day_key(location_id: int, d: date) -> str:
    """Archive key of a location-day."""
    return (f"records/csv.gz/locationid={location_id}/year={d.year:04d}/month={d.month:02d}/"
            f"location-{location_id}-{d:%Y%m%d}.csv.gz")  # fmt: skip


def _q(conn: Conn, sql: str, *args: object) -> list[tuple[Any, ...]]:
    return conn.execute(sql, args).fetchall()


def collect(
    conn: Conn,
    pipe: Pipeline,
    summary: RunSummary,
    expected: set[tuple[int, date]],
    source: Source,
    landing: Path,
    now: datetime,
    seen_revisions: set[int],
) -> list[Evidence]:
    """Evidence for every expected location-day that is not cleanly published."""
    out: list[Evidence] = []
    errors = {(e["location_id"], e["source_date"]): e for e in summary.errors}
    for loc, d in sorted(expected):
        err = errors.get((loc, d.isoformat()))
        current = pipe.current_batch(conn, loc, d)
        if err is not None:
            stages = sorted(pipe.done(conn, current)) if current else []
            out.append(Evidence("run_error", loc, d, current, {**err, "stages_done": stages}))
            continue
        if current is None:
            rejected = _q(
                conn,
                "SELECT batch_id, status_reason, sha256 FROM ops.batch_manifest"
                " WHERE location_id = %s AND source_date = %s AND status = 'rejected'"
                " ORDER BY revision_no DESC LIMIT 1",
                loc,
                d,
            )
            src = source.get(day_key(loc, d))
            if rejected:
                bid, reason, sha = rejected[0]
                out.append(
                    Evidence(
                        "rejected_file",
                        loc,
                        d,
                        bid,
                        {
                            "reason": reason,
                            "landing_sha256": sha,
                            "source_sha256": sha256_hex(src) if src is not None else None,
                        },
                    )
                )
            else:
                tz = _q(conn, "SELECT timezone FROM wh.dim_location WHERE location_id = %s", loc)
                day_end = datetime.combine(
                    d + timedelta(days=1), datetime.min.time(), tzinfo=UTC
                )  # tz offset < 1 day: conservative bound
                out.append(
                    Evidence(
                        "no_file",
                        loc,
                        d,
                        None,
                        {
                            "in_source": src is not None,
                            "timezone": tz[0][0] if tz else None,
                            "hours_since_day_end": round((now - day_end).total_seconds() / 3600, 1),
                            "in_landing": (landing / day_key(loc, d)).exists(),
                        },
                    )
                )
            continue
        status, rev = _q(
            conn, "SELECT status, revision_no FROM ops.batch_manifest WHERE batch_id = %s", current
        )[0]
        done = pipe.done(conn, current)
        if status == "blocked":
            detail = _q(
                conn,
                "SELECT detail FROM ops.checkpoint WHERE batch_id = %s AND stage = 'quality'",
                current,
            )
            out.append(Evidence("blocked", loc, d, current, detail[0][0] if detail else {}))
        elif "verified" not in done:
            out.append(Evidence("not_verified", loc, d, current, {"stages_done": sorted(done)}))
        elif rev > 0 and current not in seen_revisions:
            superseded = _q(
                conn, "SELECT count(*) FROM ops.batch_manifest WHERE superseded_by = %s", current
            )[0][0]
            if superseded:
                out.append(Evidence("revision", loc, d, current, {"revision_no": rev}))
    return out


def classify(ev: Evidence) -> str:
    """Deterministic classifier clf-1: evidence -> failure class. Unknown -> 'unknown'."""
    if ev.kind == "run_error":
        if ev.data.get("transient") or ev.data.get("error") in TRANSIENT_ERRORS:
            return "transient_db"
        if ev.data.get("error") == "InjectedCrash" or ev.data.get("stages_done") is not None:
            return "process_crash"
        return "unknown"
    if ev.kind == "rejected_file":
        reason = str(ev.data.get("reason", ""))
        if reason.startswith("unknown_header"):
            return "unknown_schema"
        if reason.startswith(("unreadable", "empty")):
            src, land = ev.data.get("source_sha256"), ev.data.get("landing_sha256")
            return "transfer_corruption" if src is not None and src != land else "source_unreadable"
        return "unknown"
    if ev.kind == "no_file":
        if ev.data.get("in_source"):
            return "missing_file"
        if float(ev.data.get("hours_since_day_end", 0)) <= GRACE_HOURS_AFTER_DAY_END:
            return "late_file"
        return "unknown"  # past grace and not upstream: a human must look
    if ev.kind == "blocked":
        counts = {k: v for k, v in dict(ev.data.get("counts", {})).items()
                  if k.startswith("Q") and k not in {"Q12_high_band", "Q13_stuck",
                                                     "Q14_spike_hampel", "Q15_relocated",
                                                     "Q16_out_of_scope"}}  # fmt: skip
        top = max(counts, key=lambda k: counts[k]) if counts else ""
        if top == "Q05_unknown_unit":
            return "unknown_unit"
        if top == "Q08_offset_mismatch":
            return "timezone_offset"
        return "rows_over_threshold"
    if ev.kind == "not_verified":
        return "verification_failed"
    if ev.kind == "revision":
        return "revision"
    return "unknown"


@dataclass
class IncidentRecord:
    """What happened to one incident (also persisted in ops.incident)."""

    incident_id: int
    evidence: Evidence
    failure_class: str
    rule: PolicyRule
    detected_at: float
    actions: list[str] = field(default_factory=list)
    attempts: int = 0
    verified: bool = False
    escalated: bool = False
    resolved_at: float | None = None
    acted_in_run: int = -1  # wait_and_recheck acts at most once per scheduled run

    @property
    def day(self) -> tuple[int, date]:
        return (self.evidence.location_id, self.evidence.source_date)


@dataclass
class Engine:
    """The guarded recovery engine for one pipeline + source."""

    pipe: Pipeline
    policy: Policy
    source: Source
    landing: Path
    connect: Callable[[], Conn]
    origin: str = "real"
    run_id: int | None = None
    sleep: Callable[[float], None] = time.sleep
    backoff_scale: float = 1.0
    clock: Callable[[], float] = time.monotonic
    open: dict[tuple[str, int, date, int | None], IncidentRecord] = field(default_factory=dict)
    closed: list[IncidentRecord] = field(default_factory=list)
    seen_revisions: set[int] = field(default_factory=set)
    current_run: int = 0

    # --------------------------------------------------------------- record
    def _record(self, conn: Conn, ev: Evidence, fclass: str, rule: PolicyRule) -> int:
        decision = "escalate" if rule.action == "escalate" else "act"
        row = conn.execute(
            "INSERT INTO ops.incident (location_id, source_date, batch_id, stage, failure_class,"
            " evidence, classifier_version, decision, policy_rule_id, action, origin, run_id)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING incident_id",
            (
                ev.location_id,
                ev.source_date,
                ev.batch_id,
                ev.kind,
                fclass,
                Jsonb({"kind": ev.kind, **ev.data}),
                CLASSIFIER_VERSION,
                decision,
                rule.id,
                rule.action,
                self.origin,
                self.run_id,
            ),
        ).fetchone()
        conn.commit()
        if row is None:  # pragma: no cover - INSERT ... RETURNING always returns a row
            raise RuntimeError("incident insert returned no id")
        return int(row[0])

    def _log(
        self,
        conn: Conn,
        inc: IncidentRecord,
        action: str,
        result: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO ops.action_log"
            " (incident_id, actor, action, policy_rule_id, params, result)"
            " VALUES (%s, 'policy-engine', %s, %s, %s, %s)",
            (inc.incident_id, action, inc.rule.id, Jsonb(params or {}), result),
        )
        conn.commit()

    def _close(self, conn: Conn, inc: IncidentRecord, outcome: str) -> None:
        ttr = (inc.resolved_at - inc.detected_at) if inc.resolved_at is not None else None
        conn.execute(
            "UPDATE ops.incident SET attempts = %s, outcome = %s, verified = %s, escalated = %s,"
            " time_to_verified_recovery_s = %s WHERE incident_id = %s",
            (inc.attempts, outcome, inc.verified, inc.escalated, ttr, inc.incident_id),
        )
        conn.commit()

    # ---------------------------------------------------------------- handle
    def handle(self, conn: Conn, evidence: list[Evidence]) -> Conn:
        """One decision per piece of evidence. Returns a usable connection (may be new)."""
        for ev in evidence:
            # Later evidence for a location-day with an open incident belongs to that
            # incident (e.g. a crash that becomes "not verified" after a failed resume).
            inc = next(
                (i for i in self.open.values() if i.day == (ev.location_id, ev.source_date)), None
            )
            if inc is None:
                fclass = classify(ev)
                rule = self.policy.for_class(fclass)
                inc = IncidentRecord(
                    self._record(conn, ev, fclass, rule), ev, fclass, rule, self.clock()
                )
                self.open[ev.key] = inc
            if inc.rule.action == "wait_and_recheck" and inc.acted_in_run == self.current_run:
                continue
            inc.acted_in_run = self.current_run
            conn = self._act(conn, inc)
        return conn

    def _escalate(self, conn: Conn, inc: IncidentRecord, why: str) -> None:
        inc.escalated = True
        self._log(
            conn, inc, "escalate", why, {"runbook": inc.rule.runbook, "evidence": inc.evidence.data}
        )
        self._close(conn, inc, "escalated")
        self.closed.append(self.open.pop(inc.evidence.key))

    def _act(self, conn: Conn, inc: IncidentRecord) -> Conn:
        rule, ev = inc.rule, inc.evidence
        if rule.action == "escalate":
            self._escalate(conn, inc, "policy requires a human decision")
            return conn
        if inc.attempts >= rule.max_attempts:
            self._escalate(conn, inc, f"{rule.action}: {inc.attempts} attempts exhausted")
            return conn
        inc.attempts += 1
        inc.actions.append(rule.action)
        if rule.backoff_s and inc.attempts > 1:
            self.sleep(rule.backoff_s * self.backoff_scale * 2 ** (inc.attempts - 2))
        key = day_key(ev.location_id, ev.source_date)
        try:
            if rule.action in ("retry", "resume_from_checkpoint"):
                if conn.closed or conn.broken:
                    conn = self.connect()
                elif conn.info.transaction_status != 0:
                    conn.rollback()
                bid = self.pipe.current_batch(conn, ev.location_id, ev.source_date)
                if bid is not None:
                    self.pipe.process(conn, bid)
            elif rule.action in ("refetch_and_reload", "bounded_backfill"):
                data = self.source.get(key)
                if data is None:
                    self._escalate(conn, inc, "source has no file to fetch")
                    return conn
                write_atomic(self.landing / key, data)
                reg = self.pipe.register(conn, key, data, datetime.now(UTC))
                bid = self.pipe.current_batch(conn, ev.location_id, ev.source_date)
                if bid is not None:
                    self.pipe.process(conn, bid)
                self._log(conn, inc, rule.action, "done", {"key": key, "outcome": reg.outcome})
            elif rule.action == "wait_and_recheck":
                self._log(conn, inc, rule.action, "waiting for the next scheduled run")
                return conn  # stays open; verified when the file has arrived and published
            elif rule.action in ("apply_revision", "block_publication"):
                if ev.batch_id is not None:
                    self.seen_revisions.add(ev.batch_id)
                self._log(conn, inc, rule.action, "already enforced by the pipeline")
        except (psycopg.Error, OSError) as exc:
            self._log(
                conn if not conn.closed else self.connect(),
                inc,
                rule.action,
                f"failed: {type(exc).__name__}",
            )
            if conn.closed or conn.broken:
                conn = self.connect()
            return conn
        conn = self._verify(conn, inc)
        return conn

    def _verify(self, conn: Conn, inc: IncidentRecord) -> Conn:
        ev = inc.evidence
        bid = self.pipe.current_batch(conn, ev.location_id, ev.source_date)
        if inc.rule.action == "block_publication":
            statuses = {
                s
                for (s,) in conn.execute(
                    "SELECT status FROM ops.publication_status WHERE location_id = %s"
                    " AND local_date = %s",
                    (ev.location_id, ev.source_date),
                ).fetchall()
            }
            ok = statuses == {"blocked"}
        else:
            ok = bid is not None and self.pipe.verify(conn, bid)
        if ok:
            inc.verified = True
            inc.resolved_at = self.clock()
            if inc.rule.escalate:  # e.g. P07: block is automatic, the cause needs a human
                self._escalate(conn, inc, "blocked automatically; cause needs a human decision")
                return conn
            self._log(conn, inc, "verify", "verified")
            self._close(conn, inc, "recovered")
            self.closed.append(self.open.pop(ev.key))
        return conn

    def recheck_waiting(self, conn: Conn, evidence: list[Evidence]) -> Conn:
        """Close waiting incidents whose problem disappeared in this run."""
        still = {(e.location_id, e.source_date) for e in evidence}
        for inc in list(self.open.values()):
            if inc.rule.action == "wait_and_recheck" and inc.day not in still:
                conn = self._verify(conn, inc)
        return conn


@dataclass
class ModeResult:
    """What a mode did during one trial."""

    incidents: list[IncidentRecord]
    retries: int = 0
    run_errors: int = 0


def run_guarded(
    engine: Engine, conn: Conn, expected: set[tuple[int, date]], now: datetime, rounds: int = 3
) -> Conn:
    """One scheduled run with the guarded loop (up to `rounds` detect/act passes)."""
    summary = run_landing(
        conn, engine.pipe, engine.landing, sorted({loc for loc, _ in expected}), raise_errors=False
    )
    for _ in range(rounds):
        if conn.closed or conn.broken:
            conn = engine.connect()
        evidence = collect(
            conn,
            engine.pipe,
            summary,
            expected,
            engine.source,
            engine.landing,
            now,
            engine.seen_revisions,
        )
        conn = engine.recheck_waiting(conn, evidence)
        escalated = {i.day for i in engine.closed if i.escalated}
        recovered = {i.evidence.key for i in engine.closed if not i.escalated}
        new = [
            e
            for e in evidence
            if (e.location_id, e.source_date) not in escalated and e.key not in recovered
        ]
        actionable = [
            e
            for e in new
            if not any(
                i.day == (e.location_id, e.source_date)
                and i.rule.action == "wait_and_recheck"
                and i.acted_in_run == engine.current_run
                for i in engine.open.values()
            )
        ]
        if not actionable:
            break
        conn = engine.handle(conn, actionable)
        summary = RunSummary()  # later passes look at the database state only
    return conn


def run_retries_only(
    pipe: Pipeline,
    conn: Conn,
    connect: Callable[[], Conn],
    landing: Path,
    location_ids: list[int],
    max_retries: int = 3,
    sleep: Callable[[float], None] = time.sleep,
    backoff_s: float = 1.0,
) -> tuple[Conn, int, int]:
    """Rerun the whole run on any run error, up to max_retries. Returns (conn, retries, errors)."""
    retries = errors = 0
    for attempt in range(max_retries + 1):
        if conn.closed or conn.broken:
            conn = connect()
        summary = run_landing(conn, pipe, landing, location_ids, raise_errors=False)
        errors += len(summary.errors)
        if not summary.errors or attempt == max_retries:
            break
        retries += 1
        sleep(backoff_s * 2**attempt)
    return conn, retries, errors
