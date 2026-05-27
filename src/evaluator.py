"""
evaluator.py

Converts RuleResult objects into a structured audit log and writes
them to a Delta table.

Each row in the audit log represents one rule execution — its outcome,
counts, severity, and timestamp. This table is the source of truth for
the dashboard and any downstream alerting.

Audit log schema:
    run_id          — unique identifier for the full DQ run (all tables, all rules)
    rule_id         — from rules.yaml
    rule_type       — not_null, unique, range, etc.
    table_name      — source table the rule ran against
    column          — column checked (null for row_count)
    severity        — critical or warning
    description     — human-readable rule description
    status          — pass or fail
    total_rows      — total rows in the table at time of check
    failed_rows     — number of rows that failed this rule
    passed_rows     — number of rows that passed
    failure_rate    — failed_rows / total_rows
    evaluated_at    — UTC timestamp of the rule execution
    run_id          — shared across all rules in one pipeline run

Usage:
    from src.evaluator import evaluate_and_log

    run_id = evaluate_and_log(
        spark=spark,
        results=results,            # list[RuleResult] from run_all_rules()
        audit_log_path="dbfs:/dq_framework/audit_log"
    )
"""

import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

from src.rule_engine import RuleResult


# ---------------------------------------------------------------------------
# Audit log schema
# ---------------------------------------------------------------------------

AUDIT_LOG_SCHEMA = StructType([
    StructField("run_id",       StringType(),    nullable=False),
    StructField("rule_id",      StringType(),    nullable=False),
    StructField("rule_type",    StringType(),    nullable=False),
    StructField("table_name",   StringType(),    nullable=False),
    StructField("column",       StringType(),    nullable=True),
    StructField("severity",     StringType(),    nullable=False),
    StructField("description",  StringType(),    nullable=True),
    StructField("status",       StringType(),    nullable=False),
    StructField("total_rows",   IntegerType(),   nullable=False),
    StructField("failed_rows",  IntegerType(),   nullable=False),
    StructField("passed_rows",  IntegerType(),   nullable=False),
    StructField("failure_rate", DoubleType(),    nullable=False),
    StructField("evaluated_at", TimestampType(), nullable=False),
])


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _results_to_rows(results: list[RuleResult], run_id: str) -> list[dict]:
    """Converts a list of RuleResults into a list of dicts matching the audit schema."""
    return [
        {
            "run_id":       run_id,
            "rule_id":      r.rule_id,
            "rule_type":    r.rule_type,
            "table_name":   r.table_name,
            "column":       r.column,
            "severity":     r.severity,
            "description":  r.description,
            "status":       r.status,
            "total_rows":   r.total_rows,
            "failed_rows":  r.failed_rows,
            "passed_rows":  r.passed_rows,
            "failure_rate": r.failure_rate,
            "evaluated_at": r.evaluated_at,
        }
        for r in results
    ]


def _print_summary(results: list[RuleResult]) -> None:
    """Prints a readable run summary to the Databricks notebook output."""
    total   = len(results)
    passed  = sum(1 for r in results if r.status == "pass")
    failed  = sum(1 for r in results if r.status == "fail")
    critical_failures = [
        r for r in results if r.status == "fail" and r.severity == "critical"
    ]

    print(f"\n{'=' * 55}")
    print(f"  DQ Run Summary")
    print(f"{'=' * 55}")
    print(f"  Total rules evaluated : {total}")
    print(f"  Passed                : {passed}")
    print(f"  Failed                : {failed}")
    print(f"  Critical failures     : {len(critical_failures)}")
    print(f"{'=' * 55}")

    if failed > 0:
        print("\n  Failed rules:")
        for r in results:
            if r.status == "fail":
                print(
                    f"    [{r.severity.upper():8}]  {r.rule_id:20}  "
                    f"failed_rows={r.failed_rows:>6}  "
                    f"failure_rate={r.failure_rate:.1%}"
                )
    print()


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def evaluate_and_log(
    spark: SparkSession,
    results: list[RuleResult],
    audit_log_path: str = "dbfs:/dq_framework/audit_log",
) -> str:
    """
    Writes all RuleResults to the Delta audit log table.

    Appends to the existing audit log — each run adds new rows,
    preserving full history for trend analysis in the dashboard.

    Args:
        spark:          Active SparkSession
        results:        List of RuleResult from run_all_rules()
        audit_log_path: Delta path for the audit log table

    Returns:
        run_id — the unique identifier for this run (UUID string)
    """
    if not results:
        print("No results to log.")
        return None

    run_id = str(uuid.uuid4())

    rows = _results_to_rows(results, run_id)
    audit_df = spark.createDataFrame(rows, schema=AUDIT_LOG_SCHEMA)

    (
        audit_df
        .write
        .format("delta")
        .mode("append")
        .save(audit_log_path)
    )

    _print_summary(results)
    print(f"  Audit log written → {audit_log_path}")
    print(f"  run_id: {run_id}\n")

    return run_id


def get_latest_run(
    spark: SparkSession,
    audit_log_path: str = "dbfs:/dq_framework/audit_log",
):
    """
    Returns the audit log rows for the most recent run only.

    Useful for notebooks and dashboards that want to inspect
    the latest results without filtering manually.

    Args:
        spark:          Active SparkSession
        audit_log_path: Delta path for the audit log table

    Returns:
        DataFrame filtered to the latest run_id
    """
    audit_df = spark.read.format("delta").load(audit_log_path)

    latest_run_id = (
        audit_df
        .orderBy(F.col("evaluated_at").desc())
        .select("run_id")
        .first()[0]
    )

    return audit_df.filter(F.col("run_id") == latest_run_id)


def get_failure_trend(
    spark: SparkSession,
    audit_log_path: str = "dbfs:/dq_framework/audit_log",
    rule_id: str = None,
    table_name: str = None,
):
    """
    Returns failure rate over time for trend analysis.

    Optionally filtered by rule_id or table_name.
    Each row represents one rule execution across all historical runs.

    Args:
        spark:          Active SparkSession
        audit_log_path: Delta path for the audit log table
        rule_id:        Optional — filter to a specific rule
        table_name:     Optional — filter to a specific table

    Returns:
        DataFrame with columns: evaluated_at, rule_id, table_name,
        status, failure_rate — ordered by evaluated_at ascending
    """
    audit_df = spark.read.format("delta").load(audit_log_path)

    if rule_id:
        audit_df = audit_df.filter(F.col("rule_id") == rule_id)
    if table_name:
        audit_df = audit_df.filter(F.col("table_name") == table_name)

    return (
        audit_df
        .select(
            "evaluated_at", "run_id", "rule_id",
            "table_name", "status", "failure_rate"
        )
        .orderBy("evaluated_at")
    )