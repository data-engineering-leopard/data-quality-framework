"""
reporter.py

Orchestrates a full DQ pipeline run — loads config, reads each table,
runs all rules, and logs results to the audit log.

This is the main entry point called from notebooks or Databricks Workflows.
It delegates all logic to config_loader, rule_engine, and evaluator —
reporter.py itself contains no rule logic.

Usage:
    from src.reporter import run_dq_pipeline

    run_id = run_dq_pipeline(
        spark=spark,
        config_path="config/rules.yaml",
        audit_log_path="dbfs:/dq_framework/audit_log",
    )
"""

from pyspark.sql import SparkSession

from src.config_loader import load_config
from src.rule_engine import run_all_rules, RuleResult
from src.evaluator import evaluate_and_log


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def run_dq_pipeline(
    spark: SparkSession,
    config_path: str = "config/rules.yaml",
    audit_log_path: str = "dbfs:/dq_framework/audit_log",
) -> tuple[str, list[RuleResult]]:
    """
    Runs the full DQ pipeline for all tables defined in the config.

    Steps:
        1. Load and validate rules.yaml
        2. For each table: read Delta table, run all rules
        3. Collect all RuleResults and write to audit log

    Args:
        spark:          Active SparkSession
        config_path:    Path to rules.yaml
        audit_log_path: Delta path for the audit log table

    Returns:
        Tuple of (run_id, list[RuleResult]) — run_id is the UUID
        for this pipeline run, results are the full list of outcomes
    """
    print(f"Loading config from: {config_path}")
    config = load_config(config_path)
    print(f"Found {len(config.tables)} table(s) to evaluate\n")

    all_results: list[RuleResult] = []

    for table in config.tables:
        print(f"Reading table: {table.name} → {table.path}")
        df = spark.read.format("delta").load(table.path)

        print(f"  Running {len(table.rules)} rule(s)...")
        results = run_all_rules(
            spark=spark,
            df=df,
            rules=table.rules,
            table_name=table.name,
        )
        all_results.extend(results)

    run_id = evaluate_and_log(
        spark=spark,
        results=all_results,
        audit_log_path=audit_log_path,
    )

    return run_id, all_results