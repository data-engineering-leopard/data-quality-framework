"""
config_loader.py

Parses and validates rules.yaml into Python dataclasses.

Provides a clean, typed interface to the rest of the framework
so rule_engine.py never has to deal with raw dicts or YAML quirks.

Usage:
    from src.config_loader import load_config

    config = load_config("config/rules.yaml")
    for table in config.tables:
        print(table.name, [r.rule_id for r in table.rules])
"""

import yaml
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path


# ---------------------------------------------------------------------------
# Dataclasses — one per config level
# ---------------------------------------------------------------------------

@dataclass
class RuleConfig:
    """Represents a single DQ rule parsed from YAML."""
    rule_id: str
    type: str
    severity: str                        # "critical" or "warning"
    description: str = ""
    column: Optional[str] = None         # not required for row_count
    pattern: Optional[str] = None        # regex only
    min: Optional[float] = None          # range only
    max: Optional[float] = None          # range only
    min_count: Optional[int] = None      # row_count only
    reference_table: Optional[str] = None    # referential_integrity only
    reference_column: Optional[str] = None   # referential_integrity only


@dataclass
class TableConfig:
    """Represents a table and its associated rules."""
    name: str
    path: str
    rules: list[RuleConfig] = field(default_factory=list)


@dataclass
class DQConfig:
    """Top-level config object returned to the framework."""
    tables: list[TableConfig] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

VALID_RULE_TYPES = {
    "not_null",
    "unique",
    "range",
    "regex",
    "row_count",
    "referential_integrity",
}

VALID_SEVERITIES = {"critical", "warning"}

REQUIRED_FIELDS_BY_TYPE = {
    "not_null":              ["column"],
    "unique":                ["column"],
    "range":                 ["column", "min", "max"],
    "regex":                 ["column", "pattern"],
    "row_count":             ["min_count"],
    "referential_integrity": ["column", "reference_table", "reference_column"],
}


def _validate_rule(rule: dict, table_name: str) -> None:
    """
    Raises ValueError if a rule dict is missing required fields
    or contains unsupported values.
    """
    rule_id = rule.get("rule_id", "<unknown>")
    prefix = f"[{table_name} / {rule_id}]"

    rule_type = rule.get("type")
    if rule_type not in VALID_RULE_TYPES:
        raise ValueError(
            f"{prefix} Unknown rule type '{rule_type}'. "
            f"Valid types: {sorted(VALID_RULE_TYPES)}"
        )

    severity = rule.get("severity")
    if severity not in VALID_SEVERITIES:
        raise ValueError(
            f"{prefix} Invalid severity '{severity}'. "
            f"Must be one of: {sorted(VALID_SEVERITIES)}"
        )

    required = REQUIRED_FIELDS_BY_TYPE[rule_type]
    for field_name in required:
        if rule.get(field_name) is None:
            raise ValueError(
                f"{prefix} Rule type '{rule_type}' requires field '{field_name}'"
            )


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> DQConfig:
    """
    Loads and validates a rules YAML file.

    Args:
        config_path: Path to rules.yaml (relative or absolute)

    Returns:
        DQConfig object containing all table and rule definitions

    Raises:
        FileNotFoundError: If the config file does not exist
        ValueError:        If any rule fails validation
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    if not raw or "tables" not in raw:
        raise ValueError("Config file must contain a top-level 'tables' key")

    tables = []
    for table_raw in raw["tables"]:
        table_name = table_raw.get("name", "<unknown>")
        table_path = table_raw.get("path")

        if not table_path:
            raise ValueError(f"Table '{table_name}' is missing a 'path' field")

        rules = []
        for rule_raw in table_raw.get("rules", []):
            _validate_rule(rule_raw, table_name)
            rules.append(RuleConfig(
                rule_id=rule_raw["rule_id"],
                type=rule_raw["type"],
                severity=rule_raw["severity"],
                description=rule_raw.get("description", ""),
                column=rule_raw.get("column"),
                pattern=rule_raw.get("pattern"),
                min=rule_raw.get("min"),
                max=rule_raw.get("max"),
                min_count=rule_raw.get("min_count"),
                reference_table=rule_raw.get("reference_table"),
                reference_column=rule_raw.get("reference_column"),
            ))

        tables.append(TableConfig(
            name=table_name,
            path=table_path,
            rules=rules,
        ))

    return DQConfig(tables=tables)