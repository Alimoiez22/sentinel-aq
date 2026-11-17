# Every step of the project is one `make` target. `make help` lists them.
# Commands run inside Docker, so results do not depend on what is installed on the host.

COMPOSE = docker compose
RUN_APP = $(COMPOSE) run --rm app
# For commands that do not need the database (M1 discovery, fetch, snapshot).
RUN_NODB = $(COMPOSE) run --rm --no-deps app

# Recipes need a POSIX shell. On Windows, make falls back to cmd.exe when no `sh` is on
# PATH (e.g. when run from PowerShell). Under cmd.exe a shell-based safety check once
# failed OPEN and `make destroy` deleted the volumes (2026-11-06, see ADR-009).
# $$((1+1)) prints 2 only in a POSIX shell.
ifneq ($(shell echo $$((1+1))),2)
$(error This Makefile needs a POSIX shell. Run make from Git Bash or WSL, not PowerShell or cmd)
endif

.PHONY: help setup build up up-airflow down status logs psql check-db fixtures db-aux migrate \
        discover coverage fetch coverage-hours snapshot test-network \
        lint fmt typecheck test test-unit test-integration demo clean destroy

## help: list the targets
help:
	@grep -E '^## ' Makefile | sed 's/^## /  /'

## setup: create .env from .env.example if it is missing, then build all images
setup:
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example; edit the passwords")
	$(COMPOSE) --profile tools --profile airflow build

## build: rebuild the app image (after changing pyproject.toml or uv.lock)
build:
	$(COMPOSE) --profile tools build app

## up: start the database and wait until it is healthy
up:
	$(COMPOSE) up -d --wait db

## up-airflow: start the database and Airflow (UI on http://127.0.0.1:8080)
up-airflow:
	$(COMPOSE) --profile airflow up -d --wait

## down: stop every container; data volumes are kept
down:
	$(COMPOSE) --profile tools --profile airflow down

## status: container state and memory use
status:
	$(COMPOSE) --profile airflow ps
	docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"

## logs: last 50 lines of every running service
logs:
	$(COMPOSE) --profile airflow logs --tail=50

## psql: interactive SQL prompt as the pipeline role (type \q to leave)
psql:
	$(COMPOSE) exec db sh -c 'psql -U "$$SENTINEL_DB_USER" -d "$$SENTINEL_DB_NAME"'

## db-aux: create warehouse_test and warehouse_exp on an existing volume (idempotent)
db-aux: up
	$(COMPOSE) exec -T db bash /docker-entrypoint-initdb.d/10_aux_databases.sh

## migrate: apply sql/ddl migrations and load guidelines into the warehouse
migrate: up
	$(RUN_APP) sentinel migrate

## check-db: connect as the pipeline role and print PostgreSQL and PostGIS versions
check-db: up
	$(RUN_APP) sentinel check-db

## fixtures: regenerate the synthetic test files (byte-identical on every run)
fixtures:
	$(RUN_APP) python scripts/make_fixtures.py

## lint: ruff lint and format check
lint:
	$(RUN_APP) sh -c "ruff check . && ruff format --check ."

## fmt: apply ruff formatting and safe fixes
fmt:
	$(RUN_APP) sh -c "ruff format . && ruff check --fix ."

## typecheck: mypy in strict mode
typecheck:
	$(RUN_APP) mypy

## test-unit: unit tests only (no database needed)
test-unit:
	$(COMPOSE) run --rm --no-deps app pytest -p no:cacheprovider -m "not integration and not network"

## test-integration: tests against the Compose database
test-integration: up
	$(RUN_APP) pytest -p no:cacheprovider -m integration

## test: lint + types + all tests. A milestone is done only when this passes.
test: up
	$(RUN_APP) sh -c "ruff check . && ruff format --check . && mypy && pytest -p no:cacheprovider -m 'not network'"

## discover: list OpenAQ locations of the scope country -> results/m1_locations.csv (needs OPENAQ_API_KEY)
discover:
	$(RUN_NODB) sentinel discover

## discover-csv: discovery without the API, from config/locations_lahore.csv (provenance in file)
discover-csv:
	$(RUN_NODB) sentinel discover --from-csv config/locations_lahore.csv

## coverage: S3 listings for candidates, then the summary report (LAHORE_ONLY=yes to narrow)
coverage:
	$(RUN_NODB) sentinel coverage-listing $(if $(filter yes,$(LAHORE_ONLY)),--lahore-only,)
	$(RUN_NODB) sentinel coverage-report

## fetch: plan downloads for LOCATIONS="id id"; add CONFIRM=yes to download
fetch:
ifndef LOCATIONS
	$(error Usage: make fetch LOCATIONS="123 456" [CONFIRM=yes])
endif
	$(RUN_NODB) sentinel fetch --locations "$(LOCATIONS)" $(if $(filter yes,$(CONFIRM)),--confirm,)

## coverage-hours: hours per day from fetched files, then the summary report
coverage-hours:
	$(RUN_NODB) sentinel coverage-hours
	$(RUN_NODB) sentinel coverage-report

## snapshot: Track B daily snapshot of config/scope.yaml snapshot_locations
snapshot:
	$(RUN_NODB) sentinel snapshot

## test-network: smoke test against the real OpenAQ bucket (not part of make test)
test-network:
	$(RUN_NODB) pytest -p no:cacheprovider -m network

## experiment: Track A, every fault x automated mode x seed -> experiments/results/
experiment: up
	$(RUN_APP) sh -c "SENTINEL_LOG_LEVEL=ERROR sentinel experiment"

## experiment-real: Track A on real Lahore files (config/experiments_real.yaml)
experiment-real: up
	$(RUN_APP) sh -c "SENTINEL_LOG_LEVEL=ERROR sentinel experiment --config config/experiments_real.yaml"

## lahore-report: Lahore PM2.5 by month and winter, from the verified marts
lahore-report: up
	$(RUN_APP) sentinel lahore-report

## reprocess: replay quality..verify for scope locations (after a rule version change)
reprocess: up
	$(RUN_APP) sh -c "SENTINEL_LOG_LEVEL=WARNING sentinel reprocess"

## golden: rebuild the golden schema from the latest raw revisions
golden: up
	$(RUN_APP) sentinel golden

## reconcile: compare published tables with golden (exit 1 if they differ)
reconcile: up
	$(RUN_APP) sentinel reconcile

## demo: 3-day guarded run with an injected fault and a revision (warehouse_exp)
demo: up db-aux
	$(RUN_APP) sh -c "SENTINEL_LOG_LEVEL=ERROR sentinel demo"

## analyse: summary tables and figures from experiments/results/results.csv
analyse:
	$(RUN_NODB) sentinel analyse
	@if [ -f experiments/results_real/results.csv ]; then $(RUN_NODB) sentinel analyse --dir experiments/results_real --tag real; fi

## revisions-report: Track B report from data/snapshots and the live warehouse
revisions-report: up
	$(RUN_APP) sentinel revisions-report

## dashboard: static HTML over the ops views (DB=warehouse_exp for experiment data)
dashboard: up
	$(RUN_APP) sentinel dashboard $(if $(DB),--db $(DB),)

## manual-start: manual-mode trial, e.g. make manual-start FAULT=F2 SEED=11 OPERATOR=ali
manual-start: up db-aux
ifndef OPERATOR
	$(error Usage: make manual-start FAULT=F2 SEED=11 OPERATOR=<name>)
endif
	$(RUN_APP) sentinel manual-start --fault $(FAULT) --seed $(SEED) --operator $(OPERATOR)

## manual-shell: shell on warehouse_exp for the operator of a manual trial
manual-shell: up
	$(COMPOSE) run --rm -e SENTINEL_DB_NAME=warehouse_exp app bash

## manual-finish: stop the clock and record the manual trial
manual-finish: up
	$(RUN_APP) sentinel manual-finish

## clean: stop containers and remove local caches; never touches data/ or volumes
clean: down
	rm -rf .pytest_cache .ruff_cache .mypy_cache

## destroy: delete the database and Airflow volumes. Needs CONFIRM=yes
# The guard is evaluated by make itself, not by a shell, so it cannot fail open.
destroy:
ifneq ($(CONFIRM),yes)
	$(error This deletes all database data. Run: make destroy CONFIRM=yes)
endif
	$(COMPOSE) --profile tools --profile airflow down -v
