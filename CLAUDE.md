# Working rules for this repository

## Who I am and why this exists
I am building portfolio projects for Erasmus Mundus DEAI (Data Engineering & AI) and
similar master's scholarships (EDISS, KAUST, MBZUAI, TUM). Reviewers are database and
data-engineering professors. They value rigor, correctness, reproducibility, sound
methodology and honest evaluation over feature count or buzzwords. I must be able to
explain every design decision in an interview, so teach me as you build.

## How you must work
1. PLAN FIRST. Before coding a milestone, show a short plan: files, design choices, risks.
   Wait for my "go".
2. ONE MILESTONE AT A TIME. Never start the next one until the current one passes its
   acceptance criteria and I confirm.
3. TESTS ARE MANDATORY. pytest for every module; run them and show results.
   A milestone is done only when `make test` passes.
4. NEVER FAKE RESULTS. Never invent numbers, metrics, dataset facts or citations.
   Every number in docs must come from a file the code produced (results/*.csv).
   If something was not measured, write "not measured".
5. VERIFY, DON'T ASSUME. Check real data headers, library APIs and tool versions before
   relying on them. Pin versions in lockfiles.
6. DOCUMENT DECISIONS in docs/DECISIONS.md (ADR style: context, options, decision,
   consequences), in plain English.
7. EXPLAIN AS YOU GO. After each milestone: a 5–10 line summary (what, why, what I must
   understand) + 3 questions an examiner might ask me.
8. SAFETY. Never delete raw data or run destructive commands without asking.
   No secrets in git; use .env + .env.example.
9. REPRODUCIBILITY. Everything runs via `make` inside Docker Compose. A fresh clone +
   `make setup && make demo` must work. Fixed seeds. Record hardware, software versions
   and git commit with every result.
10. DO NOT WRITE MY REPORT PROSE OR COVER LETTER. Outlines, tables, figures and bullet
    notes in docs/report_notes.md only. I write the final text myself.
11. CODE QUALITY. Python 3.12, type hints, ruff + mypy clean, small functions,
    docstrings, SQL in .sql files, structured JSON logs.
12. COMMITS. Suggest a conventional commit message at the end of each milestone.

## Research protocol (applies to every "Milestone R")
Goal: ground every design decision in credible sources BEFORE building.
1. SOURCES, in priority order: (a) peer-reviewed papers (VLDB, SIGMOD, ICDE, EDBT, CIDR,
   ICSE, EuroSys, NeurIPS, KDD, recognised journals); (b) official documentation of the
   tools and datasets; (c) authoritative books; (d) reputable technical reports.
   Blogs and vendor marketing only as context, clearly labelled, never as evidence.
2. VERIFY EVERY CITATION. Open the source and confirm title, authors, venue, year and the
   specific claim. If you cannot verify a source, do not cite it; list it under
   "Unverified leads" instead. Never invent DOIs or quotes.
3. PARAPHRASE. Summarise in your own words; quote only very short phrases if essential.
4. OUTPUT docs/RESEARCH.md (research questions, background, related work table, gap,
   data and tool facts, design implications, evaluation plan, threats to validity,
   unverified leads) and docs/references.bib (verified sources only).
5. LENGTH: roughly 1,500–3,000 words. STOP after R and wait for review before coding.

## Project-specific
The full project brief (research question, scope, architecture, milestones M0–M14,
definition of done) is in docs/PROJECT_BRIEF.md. Read it before starting a milestone.
