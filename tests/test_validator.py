"""
Unit Tests - Data Contract Validation Pipeline

Tests cover the rule engine in isolation (no file I/O needed) using
small in-memory DataFrames.  Each test class targets one rule category.

Run with:
    cd jpm_assessment
    python -m pytest tests/ -v
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from validator import ContractValidator



# Shared Spark session

@pytest.fixture(scope="session")
def spark():
    """Single SparkSession shared across all tests — startup is slow so reuse it."""
    session = (
        SparkSession.builder
        .appName("TestSuite")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def contract_path():
    return os.path.join(os.path.dirname(__file__), "..", "Contract_rules.yaml")


@pytest.fixture(scope="session")
def validator(contract_path):
    return ContractValidator(contract_path)



# Helper — explicit schema so None values work correctly in Spark 4


# All source columns are StringType (mirrors how CSV ingestion works).
# _delimiter_issue is BooleanType because it's set by the pipeline, not CSV.
ROW_SCHEMA = StructType([
    StructField("record_id",         StringType(),  True),
    StructField("event_ts",          StringType(),  True),
    StructField("business_date",     StringType(),  True),
    StructField("source_system",     StringType(),  True),
    StructField("currency",          StringType(),  True),
    StructField("amount",            StringType(),  True),
    StructField("status",            StringType(),  True),
    StructField("merchant_category", StringType(),  True),
    StructField("country_code",      StringType(),  True),
    StructField("_source_file",      StringType(),  True),
    StructField("_ingested_at",      StringType(),  True),
    StructField("_delimiter_issue",  BooleanType(), True),
])

VALID_BASE = {
    "record_id":         "TEST001",
    "event_ts":          "2026-05-20T08:00:00Z",
    "business_date":     "2026-05-20",
    "source_system":     "CARD_PROC",
    "currency":          "USD",
    "amount":            "100.00",
    "status":            "SETTLED",
    "merchant_category": "GROCERY",
    "country_code":      "US",
    "_source_file":      "test.csv",
    "_ingested_at":      "2026-05-20T00:00:00",
    "_delimiter_issue":  False,
}


def make_valid_row(spark, overrides: dict = None):
    """
    Build a one-row DataFrame that passes all contract rules.
    Pass overrides to set specific fields to bad values for testing.
    Uses an explicit schema so None values don't confuse Spark's type inference.
    """
    row = {**VALID_BASE, **(overrides or {})}
    # Build tuple in schema field order
    row_tuple = tuple(row[f.name] for f in ROW_SCHEMA)
    return spark.createDataFrame([row_tuple], schema=ROW_SCHEMA)



# Test: required fields


class TestRequiredFields:

    def test_valid_row_passes(self, spark, validator):
        """A fully populated valid row should have no violations."""
        df = make_valid_row(spark)
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is True
        assert result["rejection_reasons"] == ""

    def test_null_record_id_fails(self, spark, validator):
        df = make_valid_row(spark, {"record_id": None})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False
        assert "record_id" in result["rejection_reasons"]

    def test_null_event_ts_fails(self, spark, validator):
        df = make_valid_row(spark, {"event_ts": None})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False
        assert "event_ts" in result["rejection_reasons"]

    def test_whitespace_country_code_fails(self, spark, validator):
        """A space-only string should be treated as missing."""
        df = make_valid_row(spark, {"country_code": " "})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False



# Test: allowed values


class TestAllowedValues:

    def test_invalid_source_system_fails(self, spark, validator):
        """CRYPTO_RAIL is not in the allowed source systems."""
        df = make_valid_row(spark, {"source_system": "CRYPTO_RAIL"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False
        assert "source_system" in result["rejection_reasons"]

    def test_lowercase_status_fails(self, spark, validator):
        """'settled' (lowercase) is not in allowed_values which are uppercase."""
        df = make_valid_row(spark, {"status": "settled"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False

    def test_unknown_status_fails(self, spark, validator):
        df = make_valid_row(spark, {"status": "UNKNOWN"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False

    def test_invalid_currency_fails(self, spark, validator):
        df = make_valid_row(spark, {"currency": "JPY"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False

    def test_all_valid_source_systems_pass(self, spark, validator):
        for system in ["CARD_PROC", "ACH_GATEWAY", "WIRE_HUB", "WALLET_X"]:
            df = make_valid_row(spark, {"source_system": system})
            result = validator.apply(df).collect()[0]
            assert result["is_valid"] is True, f"{system} should be valid"



# Test: type validation


class TestTypeValidation:

    def test_invalid_timestamp_fails(self, spark, validator):
        """'2026/05/21 10:99:11' is not a parseable timestamp."""
        df = make_valid_row(spark, {"event_ts": "2026/05/21 10:99:11"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False
        assert "event_ts" in result["rejection_reasons"]

    def test_comma_formatted_amount_fails(self, spark, validator):
        """'1,234.56' contains a comma and should fail decimal validation."""
        df = make_valid_row(spark, {"amount": "1,234.56"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False
        assert "amount" in result["rejection_reasons"]

    def test_valid_amount_passes(self, spark, validator):
        df = make_valid_row(spark, {"amount": "1234.56"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is True



# Test: regex validation


class TestRegexValidation:

    def test_three_letter_country_code_fails(self, spark, validator):
        """'USA' is 3 characters - contract requires exactly 2 uppercase letters."""
        df = make_valid_row(spark, {"country_code": "USA"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False
        assert "country_code" in result["rejection_reasons"]

    def test_lowercase_country_code_fails(self, spark, validator):
        df = make_valid_row(spark, {"country_code": "us"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False

    def test_valid_country_codes_pass(self, spark, validator):
        for code in ["US", "GB", "DE", "FR"]:
            df = make_valid_row(spark, {"country_code": code})
            result = validator.apply(df).collect()[0]
            assert result["is_valid"] is True, f"{code} should be valid"



# Test: range validation


class TestRangeValidation:

    def test_negative_amount_fails(self, spark, validator):
        df = make_valid_row(spark, {"amount": "-3.00"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False
        assert "amount" in result["rejection_reasons"]

    def test_zero_amount_failed_status_passes(self, spark, validator):
        """0.00 on a FAILED record passes range (>= 0) and the settled rule doesn't apply."""
        df = make_valid_row(spark, {"amount": "0.00", "status": "FAILED"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is True



# Test: business rules


class TestBusinessRules:

    def test_settled_with_zero_amount_fails(self, spark, validator):
        """SETTLED records with amount=0 violate settled_amount_positive."""
        df = make_valid_row(spark, {"status": "SETTLED", "amount": "0.00"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False
        assert "settled_amount_positive" in result["rejection_reasons"]

    def test_settled_with_positive_amount_passes(self, spark, validator):
        df = make_valid_row(spark, {"status": "SETTLED", "amount": "50.00"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is True

    def test_event_ts_far_from_business_date_fails(self, spark, validator):
        """event_ts more than 7 days from business_date should fail."""
        df = make_valid_row(spark, {
            "event_ts":      "2026-06-01T09:00:00Z",
            "business_date": "2026-05-20",
        })
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False
        assert "event_ts_near_business_date" in result["rejection_reasons"]

    def test_event_ts_near_business_date_passes(self, spark, validator):
        df = make_valid_row(spark, {
            "event_ts":      "2026-05-19T23:59:00Z",
            "business_date": "2026-05-20",
        })
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is True

    def test_future_business_date_fails(self, spark, validator):
        df = make_valid_row(spark, {"business_date": "2030-01-01"})
        result = validator.apply(df).collect()[0]
        assert result["is_valid"] is False
        assert "business_date_not_far_future" in result["rejection_reasons"]



# Test: duplicate detection


class TestDuplicateDetection:

    def test_duplicate_record_ids_flagged(self, spark, validator):
        """Two rows with the same record_id should both be flagged."""
        rows = [
            ("DUP001","2026-05-20T08:00:00Z","2026-05-20","CARD_PROC","USD","10.00","POSTED","FOOD","US","f1.csv","2026-05-20",False),
            ("DUP001","2026-05-21T08:00:00Z","2026-05-21","CARD_PROC","USD","20.00","POSTED","FOOD","US","f2.csv","2026-05-21",False),
        ]
        df = spark.createDataFrame(rows, schema=ROW_SCHEMA)
        results = validator.apply(df).collect()
        for row in results:
            assert row["is_valid"] is False
            assert "duplicate" in row["rejection_reasons"]

    def test_trailing_space_record_id_duplicate(self, spark, validator):
        """'R2007' and 'R2007 ' (trailing space) should both be flagged as duplicates."""
        rows = [
            ("R2007",  "2026-05-21T07:30:00Z","2026-05-21","ACH_GATEWAY","USD","15.00","POSTED","HEALTH","US","f.csv","2026-05-21",False),
            ("R2007 ", "2026-05-21T07:35:00Z","2026-05-21","ACH_GATEWAY","USD","15.00","POSTED","HEALTH","US","f.csv","2026-05-21",False),
        ]
        df = spark.createDataFrame(rows, schema=ROW_SCHEMA)
        results = validator.apply(df).collect()
        for row in results:
            assert row["is_valid"] is False, \
                f"record_id '{row['record_id']}' should be flagged as duplicate"
