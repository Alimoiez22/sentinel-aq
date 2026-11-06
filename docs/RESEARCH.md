# Sentinel-AQ — Research Brief (Milestone R)

Status: draft for review · Prepared 2026-11-05 · Citation keys refer to `docs/references.bib`.
Every source cited here was opened or located at its official record during this milestone.
Anything not verified is under §10 "Unverified leads". The archive observations in §5.2 are from
probes I ran on 2026-11-05; I give the sample size for each.

---

## 1. Research questions (restated precisely)

**RQ1 (controlled).** We inject faults F1–F15 into an incremental daily batch pipeline over OpenAQ
data. Each recovery mode M ∈ {manual runbook, retries-only, guarded automatic} runs over 5 seeds.
For each mode we measure:
(a) the share of trials whose published outputs equal a golden rebuild (verified-recovery rate);
(b) time from fault detection to verified recovery;
(c) rows that are duplicated, missing or wrong relative to the golden rebuild;
(d) the share of trials that escalated to a human;
(e) the number of inappropriate automatic actions (defined in §8);
(f) processing overhead compared with a fault-free run.

**RQ2 (observational).** Over a real snapshot window for the chosen Lahore/Pakistan locations:
how often does the OpenAQ archive revise or patch a file that was already published, how late does
the revision arrive, and how many values change? Which quality rules fire on real data, and did the
guarded pipeline handle each event correctly?

**Success criterion.** Published outputs must be correct. A mode that "finishes" with wrong numbers
counts as a failure. It is never counted as a recovery.

---

## 2. Background: concepts you must be able to explain

1. **Idempotence.** Running an operation twice has the same effect as running it once. It is the
   property that makes retry safe. The Airflow docs ask that a task give the same result on every
   re-run, behave like a database transaction, and use UPSERT rather than INSERT
   [airflow_bestpractices]. The SRE book reports that non-idempotent fix-up automation left clusters
   in inconsistent states [beyer2016sre, ch. 7].
2. **Atomic partition replacement.** You rebuild a whole unit, here a location-day, in a temporary
   table, then swap it in inside one transaction. A reader sees either the old version or the new
   one, never a mixture. Delta Lake gives the same guarantee on object stores with an ACID
   transaction log [armbrust2020delta]. In PostgreSQL a single transaction gives it directly.
3. **MERGE vs. delete+insert.** PostgreSQL 18 `MERGE` classifies each source row as matched or
   unmatched and applies exactly one action per row. If two source rows match the same target row,
   it raises an error rather than silently picking one [pg18_merge]. That error is useful: duplicates
   fail loudly. The docs point to `INSERT … ON CONFLICT` for the case of concurrent inserts.
   dbt-postgres supports `append`, `merge`, `delete+insert` and `microbatch`. Of these, only `merge`
   and `delete+insert` with a `unique_key` are safe to re-run [dbt_incremental].
4. **Checkpoints and replay.** You record durable progress per stage so a crash resumes instead of
   restarting. The SRE book names missing checkpoints as a reason periodic pipelines waste work
   [beyer2016sre, ch. 25]. Streaming systems use consistent snapshots to get exactly-once *state*
   [carbone2017flink; akidau2015dataflow]. For batch work, "exactly-once effects" comes down to
   idempotent writes plus a durable record of which inputs were applied.
5. **Advisory locks.** These are locks defined by the application. The server grants them but does
   not enforce them. Transaction-level locks release on commit or rollback. Session-level locks
   survive a rollback, which is a known trap [pg18_locks]. We use one lock per location-day so two
   loads of the same key cannot interleave.
6. **Data validation / "unit tests for data".** These are declarative constraints checked on every
   batch, for example completeness, ranges, uniqueness and schema [schelter2018deequ]. They also
   include schema-driven checks for drift and skew between batches [breck2019datavalidation] and
   dataframe-level statistical checks [bantilan2020pandera]. They detect problems. Deciding what to
   do next is a separate step.
7. **Safe automation.** Automation multiplies whatever it does, good or bad. Google's Diskerase
   incident read an empty machine list as "all machines" and wiped almost the whole CDN fleet. The
   remedies adopted afterwards were sanity checks, rate limits and idempotent operations
   [beyer2016sre, ch. 7]. *Ironies of automation*: the more you automate, the more the rare manual
   intervention matters, and the less practised the human is at it [bainbridge1983ironies].
8. **Robust outlier detection (Hampel filter).** For each point, compute the median of a moving
   window and the MAD (median absolute deviation). Flag the point if it sits more than t·MAD from
   the median. With t = 0 the filter reduces to a median filter [pearson2016hampel]. It rests on
   Hampel's influence-function view of robust statistics [hampel1974influence]. Medians are used
   because a single spike cannot drag them, while it can drag a mean.
9. **Completeness rule for daily means.** A daily mean computed from 6 hours is not comparable with
   one computed from 24. EU Directive 2008/50/EC requires 75 % of hourly values (at least 18 hours)
   for a valid 24-hour value [eu2008directive50].
10. **WHO 2021 guideline semantics.** The 24-hour levels are defined as the **99th percentile** of
    the annual distribution, which allows 3–4 exceedance days per year [who2021aqg; comeap2021].
    "A day above 15 µg/m³" is therefore an *exceedance day*, not a guideline violation on its own.

---

## 3. Related work

| Source | Type / venue | What it does | Relevance | Limitation for us |
|---|---|---|---|---|
| Eshun et al. 2026 [eshun2026agentic] | arXiv preprint (cs.ET) | Surveys tooling for AI-assisted monitoring, root-cause analysis (RCA) and remediation. Proposes a vendor-agnostic reference architecture: deterministic policy checks, AI-assisted diagnosis, approval workflow, controlled remediation | Closest framing: self-healing data pipelines with policy gates | The abstract describes an architecture. It mentions no implementation, experiments or measured outcomes. Not peer-reviewed. |
| Psaier & Dustdar 2011 [psaier2011selfhealing] | Survey, *Computing* 91(1) | Principles and taxonomy of self-healing systems (from autonomic computing) | Vocabulary: detect → diagnose → repair loop | Predates modern data stacks. Not about data correctness. |
| Beyer et al. 2016, ch. 7 & 25 [beyer2016sre] | Authoritative book (Google SRE) | Automation value and risks, Diskerase, idempotence; fragility of periodic pipelines, checkpoints, thundering herd | Source of our allow-list, rate-limit, sanity-check and idempotence requirements | Experience reports, not controlled experiments. |
| Bainbridge 1983 [bainbridge1983ironies] | Journal, *Automatica* 19(6) | Automation makes the rare human intervention harder | Justifies keeping and timing a manual baseline, and escalating with evidence | Industrial process control, not software. |
| Schelter et al. 2018 [schelter2018deequ] | PVLDB 11(12) | Declarative data-quality constraints at scale, incremental validation, anomaly detection on quality metrics | Model for our contract and quality-rule layer | Detection only. Recovery is out of scope. |
| Breck et al. 2019 [breck2019datavalidation] | SysML 2019 | Schema-based validation in TFX; skew and drift detection in production | Schema-as-contract, alert on drift | ML-input focus, Google-scale. |
| Bantilan 2020 [bantilan2020pandera] | SciPy 2020 proceedings | Statistical schema validation for dataframes | The library we plan to use (Pandera) | Checks one dataframe at a time. No cross-batch state. |
| Sambasivan et al. 2021 [sambasivan2021cascades] | CHI 2021 (Best Paper) | Interviews with 53 practitioners. Data issues compound downstream ("data cascades"), often late and invisible | Motivates measuring *output* damage, not just job success | Qualitative. Domain is ML in high-stakes settings. |
| Armbrust et al. 2020 [armbrust2020delta] | PVLDB 13(12) | ACID tables over object stores via a transaction log | Atomic partition replacement; supersede/time-travel ideas for revisions | Different storage substrate (we use PostgreSQL). |
| Akidau et al. 2015 [akidau2015dataflow]; Carbone et al. 2017 [carbone2017flink] | PVLDB 8(12); PVLDB 10(12) | Correctness under out-of-order data; consistent snapshots for exactly-once state | Concepts of lateness, late data and checkpointing | Streaming. Our scope is batch. |
| Ahmed et al. 2023 [ahmed2023rca] | ICSE 2023 | LLMs (zero-shot, fine-tuned) recommend root causes and mitigations for 40k+ Microsoft incidents | Evidence on LLM RCA ability | Scored with lexical and semantic similarity to text written by engineers, not with verified repair. |
| Chen et al. 2024 [chen2024rcacopilot] | EuroSys 2024 | RCACopilot: collects diagnostics with handlers, then an LLM predicts the root-cause category. Micro-F1 0.766, Macro-F1 0.533 (GPT-4) on 653 incidents from one service | Best-case LLM classification accuracy | One service. Results vary across runs, as the authors note. |
| Xu et al. 2025 [xu2025openrca] | ICLR 2025 | OpenRCA benchmark: 335 failures, 68 GB of telemetry. Best agent scored 11.34 % fully correct | Shows how hard open-ended RCA still is for LLMs | Microservices, not data pipelines. |
| Ottosen & Kumar 2019 [ottosen2019outlier] | *Env. Sci.: Processes & Impacts* 21(4) | Compares point and contextual outlier detection, plus gap filling, for low-cost AQ sensors | Grounds the spike and stuck checks | One network, 11 months. |
| Barkjohn et al. 2021 [barkjohn2021purpleair] | *Atmos. Meas. Tech.* 14 | US-wide correction for PurpleAir PM2.5: raw readings run about 40 % high; humidity term added | Warns that low-cost values are biased. We do *not* correct (out of scope) but must label it | US calibration. Not valid for Lahore aerosol without local co-location. |

---

## 4. State of the art and the gap

**What exists.**
- *Detection* is mature: declarative data validation [schelter2018deequ; breck2019datavalidation;
  bantilan2020pandera].
- *Correctness primitives* are well understood: idempotent writes, transactional partition swaps,
  checkpoints and snapshot-based exactly-once state [armbrust2020delta; carbone2017flink;
  airflow_bestpractices].
- *Safe-automation principles* come mainly from experience reports: allow-lists, sanity checks,
  rate limits, idempotence, keeping human skill [beyer2016sre; bainbridge1983ironies].
- *Automated diagnosis* with LLMs is evaluated on cloud incidents. Accuracy is moderate on narrow
  classification (Macro-F1 0.533 [chen2024rcacopilot]) and low on open-ended RCA (≈11 %
  [xu2025openrca]).
- *Self-healing data pipelines* appear as reference architectures without measured outcomes
  [eshun2026agentic].

**The gap (stated honestly).** I found no peer-reviewed study that measures recovery modes for a
batch data pipeline by **correctness of the published output against a golden rebuild**, using a
fault catalogue together with real upstream revisions. Existing work measures detection, job
success, or how similar a suggested root cause is to an engineer's text. It does not measure
whether the numbers people read are right. This gap is **modest**. Each mechanism we use is known,
and the contribution is an evaluation artefact plus empirical evidence, not a new algorithm. The
search behind this claim was limited to this milestone, so the claim is "not found", not "does not
exist".

**Why the decision path is deterministic.** The best LLM results above are probabilistic and vary
between runs, and the authors of [chen2024rcacopilot] acknowledge this. A wrong automatic action on
data is costly and quiet [sambasivan2021cascades; beyer2016sre ch. 7]. A deterministic classifier
gives reproducible decisions. Each decision can be audited through its `policy_rule_id`, and unknown
cases escalate by construction. An LLM, if added at all, may only suggest (M14).

---

## 5. Data source facts (OpenAQ AWS archive)

### 5.1 From official documentation [openaq_aws; openaq_terms]
- Bucket `openaq-data-archive`. Path `records/csv.gz/locationid={id}/year={YYYY}/month={MM}/`,
  Hive-style `key=value` below the first two levels. The month is zero-padded.
- One gzipped CSV per **location per day**, covering all sensors at that location.
- **Docs disagree with themselves.** The column table says `sensor_id` and `unit`, while the example
  header says `sensors_id` and `units`. The naming text says `loc-{id}-{YYYYMMDD}`, while the example
  path says `location-{id}-{YYYYMMDD}.csv.gz`.
- `datetime` is ISO-8601 with an offset. `lat` and `lon` are EPSG:4326.
- Files are written **72 hours after the end of the local day** and "may be retroactively patched"
  after fetch errors or historical scrapes.
- An SNS topic in us-east-1 announces new objects.
- **Licensing varies by provider.** OpenAQ aggregates data it believes may be redistributed. Users
  must follow each provider's terms and attribute OpenAQ. The API requires a registered key.

### 5.2 Observed by probe (2026-11-05; small samples, not generalisable)
- The bucket is listable and readable anonymously over HTTPS from this machine.
- Real header of `location-2178-20220503.csv.gz` (n = 1 file):
  `location_id,sensors_id,location,datetime,lat,lon,parameter,units,value`. This matches the docs
  *example*, not the docs *table*. Values in `units` use the literal `µg/m³` (non-ASCII), so
  encoding must be handled explicitly.
- Arrival timing (location 2178, January 2024, n = 31 files): `LastModified` fell between 07:00 and
  08:34 UTC, four calendar days after the source date. This fits "72 h after local midnight" for a
  UTC−7 location.
- Object versioning: a `ListObjectVersions` probe on one key returned a single version with
  `VersionId=null`. **Consequence: an overwritten (patched) file leaves no history in the bucket.**
  Revisions can only be detected by comparing against our own snapshots. This is why Track B depends
  on `make snapshot` running daily from as early as possible.
- **Lahore coverage: not measured.** Location search needs an OpenAQ API key (anonymous v3 calls
  return 401). The M1 coverage helper must answer this. Third-party sites list Lahore monitors,
  including a US diplomatic post and many low-cost sensors, but I did not confirm OpenAQ ids for
  them, so they are not evidence.
- The archive file carries **no instrument or sensor-type column**. Whether a sensor is
  reference-grade or low-cost must come from API metadata (to verify in M1/M2).

---

## 6. Tool and version facts (official docs, checked 2026-11-05)

| Tool | Fact | Source |
|---|---|---|
| PostgreSQL | Current major version 18 (18.6). `MERGE` supports `RETURNING` with `merge_action()` and `OLD`/`NEW`. It errors on duplicate source matches | [pg18_merge] |
| PostgreSQL | Advisory locks: session- and transaction-level. `max_locks_per_transaction` bounds capacity. Locks are visible in `pg_locks` | [pg18_locks] |
| Apache Airflow | Docs version 3.3.2. Backfill is created via CLI (`airflow backfill create`), UI or API. Reprocess options are `none`/`failed`/`completed`. Backfill has its own `max_active_runs` | [airflow_backfill] |
| Apache Airflow | Best practice: tasks idempotent, no `datetime.now()` in logic, read and write explicit partitions keyed on `data_interval_start` | [airflow_bestpractices] |
| dbt-postgres | Incremental strategies: `append`, `merge`, `delete+insert`, `microbatch` | [dbt_incremental] |
| Pandera | Statistical dataframe validation, MIT licence | [bantilan2020pandera] |

Still to pin in M0 and not researched here: PostGIS, dbt-core and psycopg versions; whether
Pandera supports polars; TimescaleDB vs. native partitioning; Grafana vs. Metabase. Each gets its
own ADR in `docs/DECISIONS.md`.

---

## 7. Design implications ("because of X, we will do Y")

1. Because the OpenAQ docs contradict themselves on column names [openaq_aws] and a real file uses
   `sensors_id` and `units` (§5.2), **we will map header variants explicitly by a header hash in
   `DATA_CONTRACT.md`.** An unknown header escalates and is never guessed.
2. Because the bucket keeps no prior versions (§5.2) and files may be patched [openaq_aws], **we
   will keep immutable content-addressed raw copies (sha256) and daily snapshots.** A revision means
   a new checksum for the same (location, date).
3. Because files land about 72 h after the local day ends [openaq_aws], **a missing file is "late"
   until a configurable grace deadline (default 96 h) and "missing" only after it.** This separates
   F7 from F8 deterministically.
4. Because Airflow can re-run tasks and asks for transaction-like, partition-keyed tasks
   [airflow_bestpractices], **every load replaces exactly one location-day inside one PostgreSQL
   transaction, keyed by the logical date, never by `now()`.**
5. Because `MERGE` raises an error on duplicate source matches [pg18_merge], **we will deduplicate
   in staging with a documented rule before the swap.** The swap then fails loudly if dedup breaks.
6. Because session advisory locks survive rollback [pg18_locks], **we will use transaction-level
   locks (`pg_advisory_xact_lock`) on a hash of (location_id, date).**
7. Because non-idempotent automation corrupted state and an empty set was read as "everything"
   [beyer2016sre ch. 7], **every automatic action is idempotent, bounded (max attempts, max days per
   backfill) and rate-limited, with a sanity guard: an empty or oversized target set aborts.**
8. Because the manual path decays when it is rarely used [bainbridge1983ironies], **escalations carry
   an evidence bundle plus a RUNBOOK section, and the manual mode is timed for real.**
9. Because LLM diagnosis is inconsistent and often wrong [chen2024rcacopilot; xu2025openrca],
   **classification is deterministic and evidence-based, unknown cases escalate, and no LLM sits in
   the decision path.**
10. Because validation tools check constraints per batch [schelter2018deequ; bantilan2020pandera],
    **Pandera checks each file's contract. Cross-batch rules (stuck sensor, Hampel spike, relocation)
    run in SQL over the hourly fact.**
11. Because a spike drags a mean but not a median [hampel1974influence; pearson2016hampel], **spikes
    are detected with a Hampel filter (window and t in config) and flagged, not deleted.** Flagged
    values stay in the means unless a later decision says otherwise. This is recorded as an ADR.
12. Because the EU directive needs ≥ 75 % (18 h) of hourly values for a 24-h value
    [eu2008directive50], **a daily mean is published as "complete" only when hours_observed ≥ 18.
    Otherwise it is "partial" and is not counted toward exceedances.** The threshold is configurable.
13. Because WHO 24-h levels are 99th-percentile statements [who2021aqg; comeap2021], **marts will
    report "exceedance days (daily mean > 15 µg/m³ PM2.5 / 45 µg/m³ PM10)" and the yearly count. We
    will not label a single day a "guideline breach".**
14. Because sensor ranges differ (PMS5003 effective range 0–500, maximum ≥ 1000 µg/m³
    [plantower_pms5003]; BAM-1020 default range 1 mg/m³ [metone_bam1020]), and Lahore smog can be
    legitimately very high, **the quarantine ceiling is 1000 µg/m³ hourly per parameter
    (configurable). Values between 500 and 1000 µg/m³ are flagged, not rejected.**
15. Because low-cost readings are biased [barkjohn2021purpleair] and the archive lacks a sensor-type
    column (§5.2), **we will not calibrate. dim_sensor records the provider and sensor class where
    metadata allows, and LIMITATIONS states this.**
16. Because licensing varies by provider [openaq_terms], **dim_location stores the provider and
    licence. README attributes OpenAQ and each provider. Raw data is not committed to git.**

---

## 8. Evaluation plan

**Baselines.** (i) *manual*: the pipeline halts and the operator recovers via RUNBOOK + CLI;
(ii) *retries_only*: bounded retries with backoff for every failure and no classification, the
common default in orchestrators; (iii) *guarded*: classifier + policy engine.

**Ground truth.** `make golden` rebuilds from the latest raw revisions into a separate schema.
Comparators check row counts, order-independent checksums per partition, and value equality on the
marts.

**Metric definitions.**
- `recovered_verified` is true only if the comparator passes after recovery.
- `time_to_verified_recovery_s` runs from detection to comparator pass. It is censored at a timeout
  and reported as "not recovered".
- Damage counts are taken against golden: duplicate, missing and wrong rows.
- **Inappropriate automatic action** (proposed definition, open for your review): any automatic
  action that is (a) not allowed for the true injected fault class by `policy.yaml`, or
  (b) leaves the output further from golden than before the action, or (c) acts on a fault whose
  correct handling is escalation.

**Design.** 15 faults × 3 modes × 5 seeds, with a reset between trials. Report denominators
("5/5"), medians with IQR, and no significance tests on n = 5 beyond descriptive statistics.

**Known pitfalls.**
- *Circularity*: the same author writes the faults and the classifier. Mitigation: freeze the
  classifier before running the experiments, and add held-out fault variants (parameters not seen
  during development).
- *Retries_only strawman*: give it the same idempotent loads, so the comparison isolates the
  decision layer.
- *Manual mode*: n = 1 operator who is also the author, with learning effects across trials.
  Randomise trial order and report it as a labelled, human-operated observation.

**Track B.** Report counts with their observation window. Zero revisions in the window is a valid
result.

---

## 9. Threats to validity

- **Construct:** the injected faults are synthetic. Real failures may co-occur or take other forms.
  The golden rebuild is "correct" only relative to the latest upstream data, not to the true air
  quality.
- **Internal:** the same person designs the faults, the policy and the evaluation (see the
  pitfalls). Timings vary with laptop load, so we record hardware and versions.
- **External:** one data source, one country, a batch-only scope, and probably a short Track B
  window. Results may not carry over to streaming or larger warehouses.
- **Statistical:** 5 seeds per cell. Report spread and do not claim small differences.
- **Measurement:** the PM values themselves are not calibrated (low-cost bias). The project's
  claims concern pipeline correctness, not exposure science.

---

## 10. Unverified leads (not cited as evidence)

- OpenAQ location ids and coverage for Lahore, including the US diplomatic post (needs an API key, M1).
- *Designing Data-Intensive Applications*, 2nd ed. [kleppmann2026ddia] exists (Kleppmann &
  Riccomini, O'Reilly 2026, batch processing in ch. 11). **I did not verify its specific idempotence
  text**, so it is cited only for batch-processing background.
- Final EuroSys 2024 title of [chen2024rcacopilot]: arXiv uses "Automatic Root Cause Analysis via
  Large Language Models for Cloud Incidents". A later draft says "Empowering Practical…". The ACM DL
  page was not accessible.
- The "92 % prevalence" figure for data cascades (seen only in secondary pages).
- OpenRCA 2.0 (≈20.7 % exact root-cause recovery). Seen in one search result only.
- Data-contract standards (e.g., the Open Data Contract Standard), Great Expectations, dbt
  contracts. Not researched this milestone.
- The current BAM-1020 datasheet range (sources range from 1 to 65 mg/m³) and the instrument
  actually used at any Lahore site.
- Ramirez-Espinosa et al. 2024 (IEEE IoT J.) and an EGUsphere 2026 preprint on PM2.5 sensor
  outliers (flatlines). Possibly useful for the stuck-sensor rule, full text not read.
- Strauch 2018 retrospective on Bainbridge; Ghosh et al. self-healing survey.
- Pakistan timezone and DST history for `Asia/Karachi`: verify against IANA tzdata in M2.

---

## 11. Decisions I need from you before M0

1. Accept the proposed definition of *inappropriate automatic action* (§8)?
2. Accept 18 h / 75 % completeness from the EU directive as the cited default, given that WHO
   gives no completeness rule?
3. Accept a quarantine ceiling of 1000 µg/m³ with a flag band at 500–1000?
4. Register an OpenAQ API key (free) so the M1 coverage helper can list Lahore locations? Store it
   in `.env` only.
5. Start `make snapshot` as soon as M1 exists. Track B's value grows with every day of snapshots.
