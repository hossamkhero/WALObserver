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

load mode="mixed" rate="5" batch="20":
  MODE={{mode}} RATE_PER_SEC={{rate}} BATCH_SIZE={{batch}} ./scripts/load-generator.sh

load-hot:
  MODE=hot RATE_PER_SEC=8 BATCH_SIZE=80 ./scripts/load-generator.sh

load-non-hot:
  MODE=non_hot RATE_PER_SEC=8 BATCH_SIZE=80 ./scripts/load-generator.sh

load-burst:
  MODE=burst RATE_PER_SEC=2 BATCH_SIZE=200 ./scripts/load-generator.sh
