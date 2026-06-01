"""
Contract Validator
==================
Reads the data contract YAML and translates every rule into a PySpark
Column expression.  The pipeline calls `ContractValidator.apply(df)` and
gets back the same DataFrame with two new columns:

  is_valid          – True only if every rule passed
  rejection_reasons – pipe-delimited string of every failure message
                      (empty string for valid rows)

Why this design?
  - Config-driven: adding a new allowed value or changing a regex requires
    only a YAML edit, not a code change + deployment.
  - Vectorised: all checks run as Spark column expressions – no Python UDFs,
    no row-by-row loops.  Spark can optimise and parallelise everything.
  - Transparent: each violation produces a human-readable message that goes
    directly into the quarantine and report outputs.
"""

import re
from typing import Any

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, StringType


class ContractValidator:
    """
    Loads a data contract YAML and exposes a single `apply(df)` method that
    tags every row with its validation result.
    """

    def __init__(self, contract_path: str) -> None:
        """
        Parse the YAML contract on construction so any config errors surface
        immediately rather than at runtime when processing millions of rows.
        """
        with open(contract_path, "r") as fh:
            self._contract = yaml.safe_load(fh)

        # Build a lookup dict for schema rules keyed by column name.
        # This makes it O(1) to find a column's rules later.
        self._schema: dict[str, dict] = {
            col["name"]: col
            for col in self._contract.get("schema", [])
        }

        self._business_rules: list[dict] = self._contract.get("business_rules", [])

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def apply(self, df: DataFrame) -> DataFrame:
        """
        Run all schema checks and business rules against every row.

        Returns `df` with two additional columns:
          is_valid          (BooleanType)
          rejection_reasons (StringType)  – pipe-delimited violation messages
        """
        # Collect every (violation_message_column) expression we're going to
        # evaluate.  Each returns either a string message (if the check failed)
        # or null (if it passed).  We'll filter out the nulls at the end.
        violation_exprs = []

        # ── Schema-level checks ───────────────────────────────────────────────
        violation_exprs += self._required_checks()
        violation_exprs += self._allowed_value_checks()
        violation_exprs += self._type_checks()
        violation_exprs += self._regex_checks()
        violation_exprs += self._range_checks()

        # ── Business rules ─────────────────────────────────────────────────────
        violation_exprs += self._business_rule_checks(df)

        # ── Cross-file uniqueness check ────────────────────────────────────────
        df = self._add_duplicate_flag(df)
        violation_exprs.append(
            F.when(F.col("_is_duplicate"),
                   F.lit("record_id: duplicate record_id found across files"))
            .otherwise(F.lit(None).cast(StringType()))
        )

        # ── Combine all violation messages into one column ─────────────────────
        # array_remove drops the nulls, then array_join concatenates with |.
        all_violations = F.array(*violation_exprs)
        rejection_col  = F.array_join(
            F.array_remove(all_violations, ""), "|"
        )

        df = (
            df
            .withColumn("rejection_reasons", rejection_col)
            .withColumn("is_valid", F.col("rejection_reasons") == "")
        )

        return df

    # -------------------------------------------------------------------------
    # Schema rule builders
    # Each method returns a list of Column expressions (one per column/rule).
    # -------------------------------------------------------------------------

    def _required_checks(self) -> list:
        """
        For every column marked required:true, return a Column expression that
        produces a violation message when the value is null OR an empty/
        whitespace-only string.

        We check for whitespace strings explicitly because CSV ingestion can
        produce ' ' (space) values that pass a null check but are semantically
        missing (e.g. country_code = ' ' in the test data).
        """
        exprs = []
        for col_name, rules in self._schema.items():
            if not rules.get("required", False):
                continue

            exprs.append(
                F.when(
                    F.col(col_name).isNull() |
                    (F.trim(F.col(col_name)) == ""),
                    F.lit(f"{col_name}: required field is null or empty")
                ).otherwise(F.lit(None).cast(StringType()))
            )
        return exprs

    def _allowed_value_checks(self) -> list:
        """
        For columns with an allowed_values list, flag any row whose value
        is not in that list.

        We do NOT normalise case before comparison.  The contract specifies
        exact allowed values (e.g. 'SETTLED') and the source system is
        expected to send them in that exact case.  Lowercase variants like
        'settled' or 'card_proc' are genuine data quality violations – they
        indicate a source system bug that the owning team needs to fix.

        Note: we skip the check if the value is null (caught by required check).
        """
        exprs = []
        for col_name, rules in self._schema.items():
            allowed = rules.get("allowed_values")
            if not allowed:
                continue

            allowed_exact = [str(v) for v in allowed]

            exprs.append(
                F.when(
                    F.col(col_name).isNotNull() &
                    ~F.trim(F.col(col_name)).isin(allowed_exact),
                    F.concat(
                        F.lit(f"{col_name}: '"),
                        F.col(col_name),
                        F.lit(f"' not in allowed_values {allowed}")
                    )
                ).otherwise(F.lit(None).cast(StringType()))
            )
        return exprs

    def _type_checks(self) -> list:
        """
        Validate that column values can actually be parsed as their declared type.

        We do NOT cast the column – we only check castability.  This keeps bronze
        values intact (we never silently coerce bad data).

        Types handled:
          timestamp – ISO-8601 or parseable datetime string
          date      – parseable date string
          decimal   – numeric, no comma-formatting, within precision/scale
        """
        exprs = []
        for col_name, rules in self._schema.items():
            declared_type = rules.get("type", "string")

            if declared_type == "timestamp":
                # Use try_cast via F.expr() so malformed values (e.g.
                # '2026/05/21 10:99:11') return NULL rather than throwing a
                # SparkDateTimeException at runtime.  F.to_timestamp() in
                # Spark 4 uses ANSI mode by default and raises on bad input.
                exprs.append(
                    F.when(
                        F.col(col_name).isNotNull() &
                        F.expr(f"try_cast(`{col_name}` as timestamp)").isNull(),
                        F.concat(
                            F.lit(f"{col_name}: '"),
                            F.col(col_name),
                            F.lit("' is not a valid timestamp")
                        )
                    ).otherwise(F.lit(None).cast(StringType()))
                )

            elif declared_type == "date":
                # Same try_cast approach for dates
                exprs.append(
                    F.when(
                        F.col(col_name).isNotNull() &
                        F.expr(f"try_cast(`{col_name}` as date)").isNull(),
                        F.concat(
                            F.lit(f"{col_name}: '"),
                            F.col(col_name),
                            F.lit("' is not a valid date")
                        )
                    ).otherwise(F.lit(None).cast(StringType()))
                )

            elif declared_type.startswith("decimal"):
                # Reject values that contain commas (e.g. "1,234.56") –
                # these are formatting artefacts from Excel exports that would
                # silently become null on cast in some systems.
                # Use try_cast via expr() to avoid ANSI-mode exceptions on
                # values like "1,234.56" that Spark 4 would throw on with cast().
                exprs.append(
                    F.when(
                        F.col(col_name).isNotNull() & (
                            F.col(col_name).contains(",") |
                            F.expr(f"try_cast(`{col_name}` as decimal(18,2))").isNull()
                        ),
                        F.concat(
                            F.lit(f"{col_name}: '"),
                            F.col(col_name),
                            F.lit("' is not a valid decimal")
                        )
                    ).otherwise(F.lit(None).cast(StringType()))
                )

        return exprs

    def _regex_checks(self) -> list:
        """
        For columns with a regex rule, validate the value matches the pattern.

        The YAML stores the regex as a string.  We compile it via Python's re
        module just to validate it's a legal pattern, then pass it to Spark's
        rlike() for distributed evaluation.
        """
        exprs = []
        for col_name, rules in self._schema.items():
            pattern = rules.get("regex")
            if not pattern:
                continue

            # Validate the pattern is legal Python regex (catches config typos)
            try:
                re.compile(pattern)
            except re.error as exc:
                raise ValueError(
                    f"Invalid regex for column '{col_name}': {pattern}"
                ) from exc

            exprs.append(
                F.when(
                    F.col(col_name).isNotNull() &
                    ~F.col(col_name).rlike(pattern),
                    F.concat(
                        F.lit(f"{col_name}: '"),
                        F.col(col_name),
                        F.lit(f"' does not match pattern {pattern}")
                    )
                ).otherwise(F.lit(None).cast(StringType()))
            )
        return exprs

    def _range_checks(self) -> list:
        """
        Enforce min_value / max_value constraints on numeric columns.

        Values are compared as decimals.  Non-numeric values are skipped here
        (the type check above already flags them).
        """
        exprs = []
        for col_name, rules in self._schema.items():
            min_val = rules.get("min_value")
            max_val = rules.get("max_value")

            if min_val is None and max_val is None:
                continue

            numeric_col = F.col(col_name).cast(DecimalType(18, 2))

            if min_val is not None:
                exprs.append(
                    F.when(
                        F.col(col_name).isNotNull() &
                        F.expr(f"try_cast(`{col_name}` as decimal(18,2))").isNotNull() &
                        (F.expr(f"try_cast(`{col_name}` as decimal(18,2))") < float(min_val)),
                        F.concat(
                            F.lit(f"{col_name}: "),
                            F.col(col_name),
                            F.lit(f" is below min_value {min_val}")
                        )
                    ).otherwise(F.lit(None).cast(StringType()))
                )

            if max_val is not None:
                exprs.append(
                    F.when(
                        F.col(col_name).isNotNull() &
                        F.expr(f"try_cast(`{col_name}` as decimal(18,2))").isNotNull() &
                        (F.expr(f"try_cast(`{col_name}` as decimal(18,2))") > float(max_val)),
                        F.concat(
                            F.lit(f"{col_name}: "),
                            F.col(col_name),
                            F.lit(f" exceeds max_value {max_val}")
                        )
                    ).otherwise(F.lit(None).cast(StringType()))
                )

        return exprs

    # -------------------------------------------------------------------------
    # Business rules
    # -------------------------------------------------------------------------

    def _business_rule_checks(self, df: DataFrame) -> list:
        """
        Translate the business_rules section of the YAML into Column expressions.

        Each business rule has:
          name      – identifier used in violation messages
          condition – optional precondition (rule only applies when this is true)
          assert    – the expression that must be true for valid rows

        We evaluate each rule as a Spark SQL expression string using expr().
        This lets the YAML author write natural SQL-like conditions without
        needing to know the PySpark Column API.
        """
        exprs = []

        for rule in self._business_rules:
            rule_name  = rule["name"]
            assert_sql = rule["assert"]
            cond_sql   = rule.get("condition")   # may be None if no precondition

            # Build the full Spark SQL predicate for this rule
            violation_expr = self._build_business_rule_expr(
                rule_name, assert_sql, cond_sql
            )
            exprs.append(violation_expr)

        return exprs

    def _build_business_rule_expr(
        self,
        rule_name: str,
        assert_sql: str,
        condition_sql: str | None,
    ):
        """
        Build a single Column expression for one business rule.

        Logic:
          IF condition is met (or there's no condition)
            AND the assertion fails
          THEN return a violation message
          ELSE return null (no violation)

        We handle the known rules from the contract explicitly so we can
        produce clear, actionable messages.  Unknown rules fall back to a
        generic SQL evaluation.
        """

        # ── settled_amount_positive ──────────────────────────────────────────
        # "SETTLED records must have amount > 0"
        if rule_name == "settled_amount_positive":
            return F.when(
                (F.upper(F.trim(F.col("status"))) == "SETTLED") &
                F.expr("try_cast(amount as decimal(18,2))").isNotNull() &
                (F.expr("try_cast(amount as decimal(18,2))") <= 0),
                F.lit(
                    f"business_rule: {rule_name} – "
                    "amount must be > 0 when status=SETTLED"
                )
            ).otherwise(F.lit(None).cast(StringType()))

        # ── event_ts_near_business_date ──────────────────────────────────────
        # "event_ts must be within 7 days of business_date"
        elif rule_name == "event_ts_near_business_date":
            return F.when(
                F.expr("try_cast(event_ts as timestamp)").isNotNull() &
                F.expr("try_cast(business_date as date)").isNotNull() &
                (
                    F.abs(
                        F.datediff(
                            F.to_date(F.expr("try_cast(event_ts as timestamp)")),
                            F.expr("try_cast(business_date as date)")
                        )
                    ) > 7
                ),
                F.concat(
                    F.lit(f"business_rule: {rule_name} – event_ts "),
                    F.col("event_ts"),
                    F.lit(" is more than 7 days from business_date "),
                    F.col("business_date")
                )
            ).otherwise(F.lit(None).cast(StringType()))

        # ── business_date_not_far_future ─────────────────────────────────────
        # "business_date must not be more than 1 day in the future"
        elif rule_name == "business_date_not_far_future":
            return F.when(
                F.expr("try_cast(business_date as date)").isNotNull() &
                (
                    F.expr("try_cast(business_date as date)") >
                    F.date_add(F.current_date(), 1)
                ),
                F.concat(
                    F.lit(f"business_rule: {rule_name} – business_date "),
                    F.col("business_date"),
                    F.lit(" is more than 1 day in the future")
                )
            ).otherwise(F.lit(None).cast(StringType()))

        # ── Generic fallback ─────────────────────────────────────────────────
        # For any future rules added to the YAML that we haven't coded
        # explicitly, attempt a raw SQL evaluation.  This keeps the system
        # extensible without code changes for simple rule additions.
        else:
            try:
                assert_col = F.expr(assert_sql)
                if condition_sql:
                    cond_col = F.expr(condition_sql)
                    return F.when(
                        cond_col & ~assert_col,
                        F.lit(f"business_rule: {rule_name} failed")
                    ).otherwise(F.lit(None).cast(StringType()))
                else:
                    return F.when(
                        ~assert_col,
                        F.lit(f"business_rule: {rule_name} failed")
                    ).otherwise(F.lit(None).cast(StringType()))
            except Exception:
                # If the SQL expression can't be parsed, skip this rule but
                # log a warning so it doesn't silently disappear.
                import logging
                logging.getLogger(__name__).warning(
                    "Could not evaluate business rule '%s' – skipping", rule_name
                )
                return F.lit(None).cast(StringType())

    # -------------------------------------------------------------------------
    # Cross-file duplicate detection
    # -------------------------------------------------------------------------

    def _add_duplicate_flag(self, df: DataFrame) -> DataFrame:
        """
        Flag rows whose record_id appears more than once across ALL input files.

        In the test data, R1003 appears in both the May-20 and May-21 files –
        this is a cross-file duplicate that a single-file uniqueness check
        would miss.

        We use a window function to count occurrences of each record_id,
        then mark any row where the count > 1 as a duplicate.

        Note: we trim the record_id to handle the 'R2007 ' (trailing space)
        test case – that should match 'R2007' and both should be flagged.
        """
        from pyspark.sql.window import Window

        # Normalise record_id by trimming whitespace before dedup check
        df = df.withColumn("_record_id_clean", F.trim(F.col("record_id")))

        # Count how many times each clean record_id appears in the entire dataset
        window = Window.partitionBy("_record_id_clean")
        df = df.withColumn("_record_id_count", F.count("*").over(window))

        # A row is a duplicate if its clean record_id appears more than once
        df = df.withColumn(
            "_is_duplicate",
            F.col("_record_id_count") > 1
        )

        # Drop the helper columns – they've served their purpose
        df = df.drop("_record_id_clean", "_record_id_count")

        return df
