set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
  @just --list

db-init:
  ./scripts/init-db.sh

db-start:
  ./scripts/start-db.sh

db-stop:
  ./scripts/stop-db.sh

db-reset:
  ./scripts/reset-db.sh

db-shell:
  ./scripts/db-shell.sh

load:
  ./scripts/load-generator.sh
