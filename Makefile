# Every step of the project is one `make` target. `make help` lists them.
# Commands run inside Docker, so results do not depend on what is installed on the host.

COMPOSE = docker compose
RUN_APP = $(COMPOSE) run --rm app

# Recipes need a POSIX shell. On Windows, make falls back to cmd.exe when no `sh` is on
# PATH (e.g. when run from PowerShell). Under cmd.exe a shell-based safety check once
# failed OPEN and `make destroy` deleted the volumes (2026-11-06, see ADR-009).
# $$((1+1)) prints 2 only in a POSIX shell.
ifneq ($(shell echo $$((1+1))),2)
$(error This Makefile needs a POSIX shell. Run make from Git Bash or WSL, not PowerShell or cmd)
endif

.PHONY: help setup build up up-airflow down status logs psql check-db fixtures \
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
	$(COMPOSE) run --rm --no-deps app pytest -p no:cacheprovider -m "not integration"

## test-integration: tests against the Compose database
test-integration: up
	$(RUN_APP) pytest -p no:cacheprovider -m integration

## test: lint + types + all tests. A milestone is done only when this passes.
test: up
	$(RUN_APP) sh -c "ruff check . && ruff format --check . && mypy && pytest -p no:cacheprovider"

## demo: end-to-end demo (available from M8)
demo:
	@echo "make demo is implemented in M8 (pipeline + injected fault + revision)."

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
