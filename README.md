# Data Contract Validation Pipeline

PySpark pipeline that validates payment records against a YAML-defined data contract using a medallion architecture.

## How it works

```
Input CSVs → [Bronze] → [Validator] → [Silver]     clean records, partitioned by business_date
                                    → [Quarantine]  rejected records with rejection_reasons
                                    → [Report]      one row per violation, CSV
```

Bronze handles delimiter normalisation (some input rows use `|` or `;` instead of `,`). The validator runs all rules on every row rather than failing fast — in a payments context you want the full picture of what's wrong with a record.

All rules live in `Contract_rules.yaml`. Adding an allowed value or tweaking a regex is a config change, not a deployment.

## Quick start (sample data)

**Local — Mac/Linux** (fastest if Python 3.11+, Java 17+, and PySpark are already installed):
```bash
pip install -r requirements.txt
make run
```

**Docker** (no local Java, Python, or PySpark setup required):
```bash
make docker-run
```

**Local — Windows:** Use Docker above, or run inside WSL (Windows Subsystem for Linux) with the Mac/Linux instructions.

**Tests:**
```bash
make test
```

## Running with your own data

**Local:**
```bash
bash run.sh /path/to/your/input /path/to/output /path/to/Contract_rules.yaml
```

**Docker Compose** (set env vars — no file editing needed):
```bash
DATA_DIR=/path/to/your/data \
CONTRACT_PATH=/path/to/your/contract.yaml \
docker compose up
```

## Backfilling a date range

Pass `start_date` and `end_date` (YYYY-MM-DD) to process only files whose names contain a matching date. Omit both to process everything in the input directory.

**Local:**
```bash
bash run.sh data/input data/output Contract_rules.yaml 2026-05-20 2026-05-21
```

**Docker:**
```bash
docker compose run --rm pipeline bash run.sh \
  /app/data/input /app/data/output /app/Contract_rules.yaml \
  2026-05-20 2026-05-21
```

## Observability

The pipeline logs row counts and duration at every stage. Two silent-failure checks run on every execution:

- **Empty input guard** — exits with a non-zero code if no rows are ingested, so orchestrators register it as a failure rather than silently writing empty outputs
- **Schema drift detection** — compares actual input columns against the contract after ingestion; logs a warning for unexpected columns and an error for missing ones

## Tuning for larger datasets

Shuffle partitions default to `4` (appropriate for the sample data). Set `SPARK_SHUFFLE_PARTITIONS` to tune for your dataset size:

```bash
SPARK_SHUFFLE_PARTITIONS=200 bash run.sh
```

Or with Docker Compose:
```bash
SPARK_SHUFFLE_PARTITIONS=200 docker compose up
```

## Structure

```
src/
  pipeline.py        # orchestration: bronze → validate → silver/quarantine/report
  validator.py       # rule engine: translates YAML rules into Spark Column expressions
tests/
  test_validator.py  # unit tests using in-memory DataFrames
data/
  input/             # sample input CSVs
  output/            # pipeline outputs (silver, quarantine, report)
Contract_rules.yaml  # all validation rules
```
