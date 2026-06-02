"""
Data Contract Validation Pipeline

JPMC Data Engineer II Take-Home Assessment

Architecture (medallion pattern):
  Input CSVs
      │
      ▼
  [BRONZE]  - raw ingestion, no changes, adds metadata columns
      │
      ▼
  [VALIDATOR] - rule engine reads Contract_rules.yaml, tags every violation
      │
      ├──► [SILVER]     - clean records only, ready for downstream consumption
      ├──► [QUARANTINE] - rejected records with rejection_reasons column
      └──► [REPORT]     - one row per violation, human-readable audit log

Design principles applied:
  - Config-driven: all rules live in the YAML, not hardcoded here
  - Idempotent: output directories are overwritten on each run (safe to re-run)
  - Non-blocking: bad records are quarantined, good records are never delayed
  - Production signals: structured logging, row counts at every stage
"""

import logging
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from validator import ContractValidator


# Logging

# Using Python's standard logger so output is structured and easy to redirect
# to a log aggregator (Splunk, CloudWatch, etc.) in production.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _filter_input_paths(input_dir: str, start_date: str, end_date: str) -> list[str]:
    """Return only CSVs whose filename date falls within [start_date, end_date]."""
    start = date.fromisoformat(start_date)
    end   = date.fromisoformat(end_date)

    matched = sorted(
        str(p) for p in Path(input_dir).glob("*.csv")
        if (m := re.search(r"(\d{4}-\d{2}-\d{2})", p.name))
        and start <= date.fromisoformat(m.group(1)) <= end
    )
    log.info("DATE FILTER - %d file(s) matched [%s → %s]",
             len(matched), start_date, end_date)
    return matched


def check_schema_drift(df: DataFrame, contract_path: str) -> None:
    """
    Compare actual DataFrame columns against the contract schema.
    Missing columns are structural failures — a whole column being absent
    means the source schema changed, not just a bad value.
    """
    with open(contract_path) as f:
        contract = yaml.safe_load(f)

    expected   = {c["name"] for c in contract.get("schema", [])}
    actual     = {c for c in df.columns if not c.startswith("_")}
    missing    = expected - actual
    unexpected = actual - expected

    if missing:
        log.error("SCHEMA DRIFT - columns missing from input: %s", sorted(missing))
    if unexpected:
        log.warning("SCHEMA DRIFT - unexpected columns in input: %s", sorted(unexpected))
    if not missing and not unexpected:
        log.info("SCHEMA - no drift detected")


# Spark session factory

def build_spark_session(app_name: str = "DataContractValidation") -> SparkSession:
    """
    Create (or reuse) a SparkSession configured for local execution.

    In production this would point at a Databricks cluster or EMR -
    the application code stays identical; only the master URL and
    config keys change.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        # local[*] = use all available CPU cores on this machine
        .master("local[*]")
        # Suppress the noisy Spark INFO logs so our own logs are readable
        .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=WARN")
        # Keep shuffle partitions small for local dev (default 200 is wasteful
        # on a laptop with <1 GB of test data)
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )


# Bronze layer - raw ingestion

def ingest_bronze(spark: SparkSession, input_dir: str) -> "DataFrame":
    """
    Read ALL CSV files in input_dir into a single DataFrame.

    Bronze = raw data exactly as received.  We make zero semantic changes
    here - every byte is preserved so we can always reprocess from this layer
    if a bug is found in later logic.

    Additions (metadata only, not source data):
      _source_file  - which file this row came from (for auditability)
      _ingested_at  - pipeline run timestamp (for lineage)
      _raw_row      - full original row as a string (for the quarantine log)
    """
    log.info("BRONZE - reading CSVs from %s", input_dir)

    # multiLine=False and sep="," are explicit defaults - being explicit makes
    # the intent clear to the next engineer who reads this.
    #
    # IMPORTANT: inferSchema=False keeps everything as StringType at this stage.
    # We intentionally do NOT coerce types in bronze - bad values like
    # "2026/05/21 10:99:11" or "1,234.56" would either fail or silently become
    # null if we let Spark infer types here.  Type validation is the validator's
    # job, not the ingestion layer's.
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # keep everything as string
        .option("mode", "PERMISSIVE")     # don't crash on malformed rows
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(input_dir)
    )

    # Attach metadata columns so every downstream layer can trace each record
    # back to its origin without reading the raw files again.
    df = (
        df
        .withColumn("_source_file",
                    F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
        .withColumn("_ingested_at",
                    F.lit(datetime.utcnow().isoformat()))
    )

    row_count = df.count()
    log.info("BRONZE - ingested %d rows from %s", row_count, input_dir)
    return df


# Pre-processing - normalise delimiter chaos before validation
def normalise_delimiters(spark: SparkSession, input_path: "str | list[str]") -> DataFrame:
    """
    The test files contain rows with mixed delimiters (pipe | and semicolon ;).
    Spark's CSV reader splits on a single separator so those rows arrive as a
    single-column string rather than 9 columns.

    Strategy:
      1. Read every file as plain text (one column = full row string).
      2. Detect the delimiter used by each row.
      3. Re-split pipe- and semicolon-delimited rows into proper columns.
      4. Union everything into one consistent DataFrame.

    This is the bronze pre-processor - it runs before we write the bronze
    layer so that bronze still contains one-row-per-record, just with a
    _delimiter_issue flag for rows that needed fixing.
    """
    log.info("BRONZE - normalising delimiters")

    # The 9 column names from the contract (order matters for re-splitting)
    COLS = [
        "record_id", "event_ts", "business_date", "source_system",
        "currency", "amount", "status", "merchant_category", "country_code"
    ]

    # ── Step 1: read standard comma-delimited rows ──────────────────────────
    # We use PERMISSIVE mode so malformed quoted rows don't crash the job.
    df_comma = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(input_path)
    )

    # Tag the source file on every row for auditability
    df_comma = (
        df_comma
        .withColumn("_source_file",
                    F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
        .withColumn("_ingested_at", F.lit(datetime.utcnow().isoformat()))
        .withColumn("_delimiter_issue", F.lit(False))
    )

    # ── Step 2: read ALL rows as raw text to catch non-comma rows ───────────
    df_text = (
        spark.read
        .option("header", "true")   # skip the header line
        .text(input_path)           # one column: "value"
        .withColumn("_source_file",
                    F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
    )

    # Identify rows that use | or ; as the primary separator
    df_bad_delim = df_text.filter(
        F.col("value").contains("|") | F.col("value").contains(";")
    )

    if df_bad_delim.count() == 0:
        # No delimiter issues found - return the clean comma-read DataFrame
        log.info("BRONZE - no delimiter anomalies detected")
        return df_comma

    # ── Step 3: re-split pipe/semicolon rows into proper columns ────────────
    # split() on either | or ; using a regex character class
    df_fixed = df_bad_delim.select(
        *[
            F.split(F.col("value"), r"[|;]").getItem(i).alias(col)
            for i, col in enumerate(COLS)
        ],
        F.col("_source_file"),
        F.lit(datetime.utcnow().isoformat()).alias("_ingested_at"),
        F.lit(True).alias("_delimiter_issue"),   # flag so we know this was fixed
    )

    # Make sure df_comma has the _delimiter_issue column before the union
    if "_delimiter_issue" not in df_comma.columns:
        df_comma = df_comma.withColumn("_delimiter_issue", F.lit(False))

    # Align schemas: df_comma may have a _corrupt_record column that df_fixed lacks
    for c in df_comma.columns:
        if c not in df_fixed.columns:
            df_fixed = df_fixed.withColumn(c, F.lit(None).cast(StringType()))
    for c in df_fixed.columns:
        if c not in df_comma.columns:
            df_comma = df_comma.withColumn(c, F.lit(None).cast(StringType()))

    # Union: keep comma rows that are NOT pipe/semicolon, plus our re-split rows
    # We identify pipe/semicolon rows in df_comma by checking if record_id
    # contains a pipe or semicolon (they arrived as a single-column blob).
    df_comma_clean = df_comma.filter(
        ~(F.col("record_id").contains("|") | F.col("record_id").contains(";"))
    )

    df_unified = df_comma_clean.unionByName(df_fixed, allowMissingColumns=True)

    fixed_count = df_bad_delim.count()
    log.info("BRONZE - re-split %d pipe/semicolon rows", fixed_count)
    return df_unified



# Validation - tag every record with its violations

def validate(df: "DataFrame", contract_path: str) -> "DataFrame":
    """
    Apply all rules from the data contract YAML to every row.

    Returns the same DataFrame with two new columns:
      is_valid          - boolean, True only if ALL rules passed
      rejection_reasons - pipe-delimited string of every rule that failed
                          (empty string for valid rows)

    We run ALL checks on every row rather than stopping at the first failure.
    This is deliberate: in a payments context you want the full picture of
    what's wrong with a record, not just the first thing that broke.
    """
    log.info("VALIDATION - loading contract from %s", contract_path)

    # ContractValidator reads the YAML and returns a list of Spark Column
    # expressions.  See validator.py for the rule implementations.
    validator = ContractValidator(contract_path)
    df_validated = validator.apply(df)

    valid_count   = df_validated.filter(F.col("is_valid")).count()
    invalid_count = df_validated.filter(~F.col("is_valid")).count()
    log.info("VALIDATION - %d valid  |  %d invalid", valid_count, invalid_count)
    return df_validated



# Silver layer - clean records only

def write_silver(df: "DataFrame", output_dir: str) -> None:
    """
    Write only the records that passed ALL validation checks.

    Silver = cleansed, trustworthy data ready for downstream consumers
    (analytics, ML feature pipelines, reporting).

    We drop the pipeline metadata columns before writing - downstream
    consumers should not need to know about _source_file etc.

    overwrite mode = idempotent.  Re-running the pipeline on the same
    input always produces the same silver output (no duplicates).
    """
    silver_path = f"{output_dir}/silver"
    log.info("SILVER - writing clean records to %s", silver_path)

    # Keep only the original contract columns (drop our pipeline metadata)
    contract_cols = [
        "record_id", "event_ts", "business_date", "source_system",
        "currency", "amount", "status", "merchant_category", "country_code",
    ]
    # Only select columns that actually exist (defensive - handles schema drift)
    cols_to_write = [c for c in contract_cols if c in df.columns]

    (
        df.filter(F.col("is_valid"))
        .select(*cols_to_write)
        # Partition by business_date so downstream queries that filter by date
        # only read the relevant partition folder - standard warehouse pattern.
        .write
        .mode("overwrite")
        .partitionBy("business_date")
        .parquet(silver_path)
    )

    count = df.filter(F.col("is_valid")).count()
    log.info("SILVER - wrote %d clean records", count)



# Quarantine layer - rejected records with reasons

def write_quarantine(df: "DataFrame", output_dir: str) -> None:
    """
    Write rejected records to the quarantine zone.

    Quarantine records include:
      - All original columns (so the source data is fully preserved)
      - rejection_reasons - pipe-delimited list of every rule that failed
      - _source_file      - which input file this came from
      - _ingested_at      - when it was ingested

    The quarantine zone feeds:
      1. The validation report (audit log for ops/compliance)
      2. Manual review / remediation workflows
      3. Potential reprocessing once upstream fixes the source system

    We do NOT fix the data here.  The quarantine preserves the exact
    bad value so the source team can diagnose their system.
    """
    quarantine_path = f"{output_dir}/quarantine"
    log.info("QUARANTINE - writing rejected records to %s", quarantine_path)

    quarantine_cols = [
        "record_id", "event_ts", "business_date", "source_system",
        "currency", "amount", "status", "merchant_category", "country_code",
        "rejection_reasons", "_source_file", "_ingested_at",
    ]
    cols_to_write = [c for c in quarantine_cols if c in df.columns]

    (
        df.filter(~F.col("is_valid"))
        .select(*cols_to_write)
        .write
        .mode("overwrite")
        .partitionBy("_source_file")   # partition by file so ops can investigate per batch
        .parquet(quarantine_path)
    )

    count = df.filter(~F.col("is_valid")).count()
    log.info("QUARANTINE - wrote %d rejected records", count)



# Validation report - human-readable audit log

def write_report(df: "DataFrame", output_dir: str) -> None:
    """
    Produce a flat CSV report: one row per violation per record.

    This is the audit artefact that ops, compliance, and source-system
    owners will actually read.  We explode the pipe-delimited
    rejection_reasons into individual rows so the report is easy to
    filter and count in Excel or a BI tool.

    Example output:
      record_id | source_file            | violation
      R2002     | payments_2026-05-21.csv | source_system: 'CRYPTO_RAIL' not in allowed_values
      R2003     | payments_2026-05-21.csv | business_rule: settled_amount_positive - amount must be > 0 when status=SETTLED
    """
    report_path = f"{output_dir}/report"
    log.info("REPORT - writing validation report to %s", report_path)

    (
        df.filter(~F.col("is_valid"))
        # Split the pipe-delimited reasons string into an array, then explode
        # so each violation gets its own row in the report.
        .withColumn("violation",
                    F.explode(F.split(F.col("rejection_reasons"), r"\|")))
        .select(
            F.col("record_id"),
            F.col("_source_file").alias("source_file"),
            F.col("business_date"),
            F.col("violation"),
        )
        # Single CSV file (coalesce(1)) so it can be opened directly in Excel.
        # In production you would NOT coalesce - let it stay partitioned.
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(report_path)
    )

    count = df.filter(~F.col("is_valid")).count()
    log.info("REPORT - %d violations across %d rejected records", count, count)



# Entrypoint=======================================================================

def run(
    input_dir: str,
    output_dir: str,
    contract_path: str,
    start_date: str = None,
    end_date: str = None,
) -> None:
    """
    Orchestrate the full pipeline:
      normalise → bronze → validate → silver + quarantine + report

    This function is the single entry point - it can be called from:
      - Command line (see __main__ block below)
      - Airflow PythonOperator
      - Databricks notebook (%run or dbutils.notebook.run)
      - Unit tests (by passing in temp directories)
    """
    log.info("Pipeline starting")
    log.info("  input_dir    : %s", input_dir)
    log.info("  output_dir   : %s", output_dir)
    log.info("  contract     : %s", contract_path)
    if start_date or end_date:
        log.info("  date range   : %s → %s", start_date or "unbounded", end_date or "unbounded")

    pipeline_start = time.perf_counter()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    input_path = (
        _filter_input_paths(input_dir, start_date, end_date)
        if start_date or end_date
        else input_dir
    )
    if isinstance(input_path, list) and not input_path:
        log.error("DATE FILTER - no files found in range [%s, %s], aborting", start_date, end_date)
        spark.stop()
        sys.exit(1)

    # ── Stage 1: Bronze ──────────────────────────────────────────────────────
    # Strictly, bronze should be untouched raw data. The current implementation 
    # makes a pragmatic trade-off — it normalises at ingestion so the structured 
    # layer is usable, and flags rows that were modified. In a production Databricks 
    # deployment, the raw files on object storage would be the true immutable bronze, 
    # and normalisation would be a separate step before validation.
    t = time.perf_counter()
    df_bronze = normalise_delimiters(spark, input_path)
    log.info("BRONZE complete in %.2fs", time.perf_counter() - t)

    bronze_count = df_bronze.count()
    if bronze_count == 0:
        log.error("BRONZE - no rows ingested from %s, aborting", input_path)
        spark.stop()
        sys.exit(1)

    check_schema_drift(df_bronze, contract_path)

    # ── Stage 2: Validate ────────────────────────────────────────────────────
    t = time.perf_counter()
    df_validated = validate(df_bronze, contract_path)
    # Cache so silver, quarantine, and report don't each retrigger the full
    # validation DAG from scratch.
    df_validated.cache()
    log.info("VALIDATE complete in %.2fs", time.perf_counter() - t)

    # ── Stage 3: Write outputs ───────────────────────────────────────────────
    # All three writes are independent - a failure in report generation does
    # NOT roll back silver or quarantine.  In production you would wrap these
    # in a transaction or use Delta Lake's ACID guarantees.
    t = time.perf_counter()
    write_silver(df_validated, output_dir)
    log.info("SILVER write complete in %.2fs", time.perf_counter() - t)

    t = time.perf_counter()
    write_quarantine(df_validated, output_dir)
    log.info("QUARANTINE write complete in %.2fs", time.perf_counter() - t)

    t = time.perf_counter()
    write_report(df_validated, output_dir)
    log.info("REPORT write complete in %.2fs", time.perf_counter() - t)

    log.info("Pipeline complete in %.2fs. Outputs at: %s",
             time.perf_counter() - pipeline_start, output_dir)
    spark.stop()


if __name__ == "__main__":
    # Allow paths to be overridden via CLI for easy local testing:
    #   python pipeline.py ../../data/input ../../data/output ../../Contract_rules.yaml
    base = Path(__file__).parent.parent

    _input      = sys.argv[1] if len(sys.argv) > 1 else str(base / "data/input")
    _output     = sys.argv[2] if len(sys.argv) > 2 else str(base / "data/output")
    _contract   = sys.argv[3] if len(sys.argv) > 3 else str(base / "Contract_rules.yaml")
    _start_date = sys.argv[4] if len(sys.argv) > 4 else None
    _end_date   = sys.argv[5] if len(sys.argv) > 5 else None

    run(_input, _output, _contract, _start_date, _end_date)
