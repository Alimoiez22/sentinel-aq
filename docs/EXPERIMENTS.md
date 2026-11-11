# Experiments

## Track A (controlled): `make experiment`
Design: `config/experiments.yaml`. Every fault F0-F15 (docs/FAULTS.md) × automated mode
(`retries_only`, `guarded`) × 5 evaluation seeds (11-15). This gives 160 trials on
`warehouse_exp`, with the database reset before each one.

Per trial:
1. reset the database, seed the two synthetic locations, start from the 6 clean
   synthetic location-days (data/fixtures/synthetic, labelled SYNTHETIC);
2. two scheduled runs (simulated times 2025-11-07 and 2025-11-08 00:00 UTC). Before each
   run, the trial's deliveries for that run are dropped into a fresh landing folder,
   and an armed crash point is set if the fault has one;
3. the mode reacts: `guarded` runs the policy engine (docs/POLICY.md); `retries_only`
   reruns the whole run on any run error, at most 3 times, and nothing else;
4. golden is rebuilt from the trial's truth inputs and compared with the published
   tables.

Outputs: `experiments/results/results.csv` (one row per trial) and `metadata.json`
(commit, versions, platform, config hashes, total wall time). `make analyse` turns them
into `summary_by_mode.csv`, `summary_by_fault_mode.csv`, `results_table.md` (also
injected into the README) and `docs/figures/*.svg`.

### Metrics (precise definitions)
| Metric | Definition |
|---|---|
| outputs_correct | every published table (fact, daily mart, monthly summary) equals golden: 0 missing, 0 extra, 0 wrong rows |
| recovered_verified | outputs_correct AND no escalation |
| escalated | at least one incident escalated to a human |
| handled_as_expected | outputs_correct AND escalated == the fault's expected escalation (FAULTS.md). Silent blocking of bad upstream data is NOT handled as expected |
| damage | missing / extra / wrong rows per table vs golden |
| inappropriate_actions | guarded: automatic actions on a fault whose expected handling is not automatic, or after a misclassification. retries_only: reruns of a fault that is not transient or a crash. (The "moved further from golden" criterion of RESEARCH §8 is not measured per action.) |
| time_to_verified_recovery_s | guarded: max over recovered incidents of detection → verification. retries_only: first run error → first clean rerun. 0 when nothing needed recovery; blank if not recovered |
| overhead_s | trial wall time minus the mean F0 wall time of the same mode |

### Honest scope notes
* **Synthetic data.** Two synthetic locations × 3 days. The numbers describe the
  recovery logic, not Lahore air quality. Real-data trials need the M1 location choice.
* **Small n.** 5 seeds per cell. We report k/n, medians and IQRs, and no significance
  tests.
* **Same author for faults and classifier.** The classifier was frozen before the
  held-out seeds ran (POLICY.md, development note). This mitigates circularity but does
  not remove it.
* **Timings** are wall-clock on one laptop inside Docker (metadata.json), with backoffs
  scaled to 0. They show relative cost, not production latency.
* **Manual mode** is not in results.csv. It needs a human operator
  (`make manual-start` / `make manual-finish`; results go to `manual_trials.csv`,
  labelled human-operated, with operator and trial number). **Not measured yet.**

## Track B (real world): `make revisions-report`
Reads every snapshot (`data/snapshots/*`) and the live warehouse. Reports revisions
(count, lateness upper bound, values changed), new files, real quality-rule hits and
real incidents, always with the observation window, and zero when zero.
**Status: not measured.** The snapshot locations need the OpenAQ API key and the M1
discovery run.
