"""
rule_engine.py

Executes DQ rules against a PySpark DataFrame.

Each rule type returns a RuleResult — a lightweight object containing
the outcome, counts, and metadata needed by the evaluator and reporter.

The engine is deliberately stateless: it takes a DataFrame and a RuleConfig,
runs the check, and returns a result. No side effects, no writes.

Usage:
    from src.config_loader import load_config
    from src.rule_engine import run_rule

    config = load_config("config/rules.yaml")
    df = spark.read.format("delta").load(table.path)

    for rule in table.rules:
        result = run_rule(spark, df, rule, table.name)
        print(result)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config_loader import RuleConfig


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class RuleResult:
    """
    Outcome of a single rule execution.

    Consumed by:
        - evaluator.py  → writes to audit log Delta table
        - quarantine.py → uses failed_df to route bad rows
    """
    rule_id: str
    rule_type: str
    table_name: str
    column: Optional[str]
    severity: str
    description: str
    status: str                  # "pass" or "fail"
    total_rows: int
    failed_rows: int
    passed_rows: int
    failure_rate: float          # 0.0 – 1.0
    evaluated_at: datetime
    failed_df: Optional[DataFrame] = None   # rows that failed (for quarantine)

    def __repr__(self) -> str:
        return (
            f"RuleResult({self.rule_id} | {self.status.upper()} | "
            f"failed={self.failed_rows}/{self.total_rows} | "
            f"severity={self.severity})"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_result(
    rule: RuleConfig,
    table_name: str,
    total_rows: int,
    failed_df: DataFrame,
) -> RuleResult:
    """Builds a RuleResult from a DataFrame of failing rows."""
    failed_rows = failed_df.count()
    passed_rows = total_rows - failed_rows
    failure_rate = failed_rows / total_rows if total_rows > 0 else 0.0
    status = "pass" if failed_rows == 0 else "fail"

    return RuleResult(
        rule_id=rule.rule_id,
        rule_type=rule.type,
        table_name=table_name,
        column=rule.column,
        severity=rule.severity,
        description=rule.description,
        status=status,
        total_rows=total_rows,
        failed_rows=failed_rows,
        passed_rows=passed_rows,
        failure_rate=round(failure_rate, 6),
        evaluated_at=datetime.utcnow(),
        failed_df=failed_df if failed_rows > 0 else None,
    )


# ---------------------------------------------------------------------------
# Rule implementations
# ---------------------------------------------------------------------------

def _check_not_null(df: DataFrame, rule: RuleConfig, table_name: str) -> RuleResult:
    """Fails rows where the specified column is null."""
    total_rows = df.count()
    failed_df = df.filter(F.col(rule.column).isNull())
    return _make_result(rule, table_name, total_rows, failed_df)


def _check_unique(df: DataFrame, rule: RuleConfig, table_name: str) -> RuleResult:
    """
    Fails rows where the column value appears more than once.
    Nulls are excluded from uniqueness checks (covered by not_null).
    """
    total_rows = df.count()

    duplicate_keys = (
        df
        .filter(F.col(rule.column).isNotNull())
        .groupBy(rule.column)
        .agg(F.count("*").alias("_count"))
        .filter(F.col("_count") > 1)
        .select(rule.column)
    )

    failed_df = df.join(duplicate_keys, on=rule.column, how="inner")
    return _make_result(rule, table_name, total_rows, failed_df)


def _check_range(df: DataFrame, rule: RuleConfig, table_name: str) -> RuleResult:
    """Fails rows where the column value falls outside [min, max]."""
    total_rows = df.count()
    failed_df = df.filter(
        F.col(rule.column).isNull() |
        (F.col(rule.column) < rule.min) |
        (F.col(rule.column) > rule.max)
    )
    return _make_result(rule, table_name, total_rows, failed_df)


def _check_regex(df: DataFrame, rule: RuleConfig, table_name: str) -> RuleResult:
    """
    Fails rows where the column value does not match the regex pattern.
    Null values are treated as failures (use not_null separately if needed).
    """
    total_rows = df.count()
    failed_df = df.filter(
        F.col(rule.column).isNull() |
        ~F.col(rule.column).rlike(rule.pattern)
    )
    return _make_result(rule, table_name, total_rows, failed_df)


def _check_row_count(df: DataFrame, rule: RuleConfig, table_name: str) -> RuleResult:
    """
    Table-level check — fails if total row count is below min_count.

    Unlike row-level rules, there are no individual failing rows.
    The entire table is considered failed if the threshold isn't met.
    failed_df is empty on pass, full table on fail.
    """
    total_rows = df.count()
    if total_rows >= rule.min_count:
        failed_df = df.filter(F.lit(False))   # empty — pass
    else:
        failed_df = df                         # whole table — fail

    return _make_result(rule, table_name, total_rows, failed_df)


def _check_referential_integrity(
    spark: SparkSession,
    df: DataFrame,
    rule: RuleConfig,
    table_name: str,
) -> RuleResult:
    """
    Fails rows where the column value does not exist in the reference table.
    Null values in the source column are treated as failures.
    """
    total_rows = df.count()

    ref_df = (
        spark.read.format("delta").load(rule.reference_table)
        .select(F.col(rule.reference_column).alias("_ref_key"))
        .distinct()
    )

    failed_df = (
        df
        .join(
            ref_df,
            on=F.col(rule.column) == F.col("_ref_key"),
            how="left"
        )
        .filter(F.col("_ref_key").isNull())
        .drop("_ref_key")
    )

    return _make_result(rule, table_name, total_rows, failed_df)


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

# Dispatch table — maps rule type string to its implementation
_RULE_DISPATCH = {
    "not_null":              lambda spark, df, rule, table: _check_not_null(df, rule, table),
    "unique":                lambda spark, df, rule, table: _check_unique(df, rule, table),
    "range":                 lambda spark, df, rule, table: _check_range(df, rule, table),
    "regex":                 lambda spark, df, rule, table: _check_regex(df, rule, table),
    "row_count":             lambda spark, df, rule, table: _check_row_count(df, rule, table),
    "referential_integrity": lambda spark, df, rule, table: _check_referential_integrity(spark, df, rule, table),
}


def run_rule(
    spark: SparkSession,
    df: DataFrame,
    rule: RuleConfig,
    table_name: str,
) -> RuleResult:
    """
    Executes a single DQ rule against a DataFrame.

    Args:
        spark:      Active SparkSession (needed for referential integrity)
        df:         Source DataFrame to check
        rule:       RuleConfig parsed from YAML
        table_name: Name of the table being checked (for result metadata)

    Returns:
        RuleResult with status, counts, and optionally a failed_df

    Raises:
        ValueError: If the rule type is not recognised
    """
    handler = _RULE_DISPATCH.get(rule.type)
    if not handler:
        raise ValueError(
            f"Unsupported rule type '{rule.type}' on rule '{rule.rule_id}'. "
            f"Supported types: {sorted(_RULE_DISPATCH.keys())}"
        )
    return handler(spark, df, rule, table_name)


def run_all_rules(
    spark: SparkSession,
    df: DataFrame,
    rules: list[RuleConfig],
    table_name: str,
) -> list[RuleResult]:
    """
    Executes all rules for a table and returns a list of RuleResults.

    Args:
        spark:      Active SparkSession
        df:         Source DataFrame for the table
        rules:      List of RuleConfig objects from the YAML config
        table_name: Name of the table being checked

    Returns:
        List of RuleResult — one per rule
    """
    results = []
    for rule in rules:
        result = run_rule(spark, df, rule, table_name)
        results.append(result)
    return results