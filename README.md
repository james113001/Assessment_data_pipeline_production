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

## Running with your own data

Pass your input directory, output directory, and contract path as arguments:

```bash
bash run.sh /path/to/your/input /path/to/output /path/to/Contract_rules.yaml
```

With Docker Compose, set env vars — no file editing needed:

```bash
DATA_DIR=/path/to/your/data \
CONTRACT_PATH=/path/to/your/contract.yaml \
docker compose up
```

## Tuning for larger datasets

Shuffle partitions default to `4` (appropriate for the sample data). Set `SPARK_SHUFFLE_PARTITIONS` to tune for your dataset size:

```bash
SPARK_SHUFFLE_PARTITIONS=200 bash run.sh
```

Or with Docker:

```bash
docker run --rm -e SPARK_SHUFFLE_PARTITIONS=200 \
  -v /path/to/your/data:/app/data \
  jpm-pipeline
```

## Quick start (sample data)

**Docker:**
```bash
make docker-run
```

**Local** (requires Python 3.11+, Java 17+):
```bash
pip install -r requirements.txt
make run
```

**Tests:**
```bash
make test
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
