# Data Quality Framework — Databricks & PySpark

A reusable, config-driven data quality framework built on Apache Spark and Delta Lake. Rules are defined in YAML and evaluated against any Delta table — no code changes required to add new checks.

Built as a standalone tool that runs independently of any pipeline, producing a centralised audit log, quarantine table, and summary dashboard across all monitored tables.

---

## Features

- **Config-driven rules** — define DQ checks in YAML, not code
- **Six rule types** — not null, unique, range, regex, row count, referential integrity
- **Centralised audit log** — every rule result written to a Delta table for trend analysis
- **Quarantine table** — failed rows routed to a separate Delta table with failure metadata
- **Summary dashboard** — pass rates, failure trends, and worst offending rules by table
- **Databricks Workflows** — fully orchestrated, runs on a schedule

---

## Tech Stack

- Python 3.10+
- Apache Spark / PySpark
- Delta Lake
- Databricks (Workflows, DBFS)
- PyYAML

---

## Project Structure

```
dq_framework/
├── config/
│   └── rules.yaml              # DQ rule definitions per table
├── src/
│   ├── config_loader.py        # Parses and validates YAML config
│   ├── rule_engine.py          # Executes rules as PySpark DataFrame checks
│   ├── evaluator.py            # Tags rows as pass/fail per rule
│   ├── quarantine.py           # Routes failed rows to quarantine table
│   └── reporter.py             # Writes results to Delta audit log
├── data/
│   └── generate_synthetic_data.py   # Generates dirty test data
├── notebooks/
│   ├── 01_setup.ipynb          # Environment setup and data generation
│   ├── 02_run_dq.ipynb         # Runs the full DQ pipeline
│   └── 03_dashboard.ipynb      # Summary dashboard and visualisations
├── tests/
│   └── test_rule_engine.py     # Unit tests for rule logic
├── requirements.txt
└── README.md
```

---

## Rule Types

| Rule | Description | Example |
|---|---|---|
| `not_null` | Column must not contain nulls | `customer_id` is required |
| `unique` | Column values must be distinct | `order_id` must be unique |
| `range` | Numeric value within min/max bounds | `price` between 0 and 10,000 |
| `regex` | Value must match a pattern | `email` matches email format |
| `row_count` | Table must meet a minimum row threshold | Table must have > 1,000 rows |
| `referential_integrity` | Value must exist in a reference table | `customer_id` must exist in customers |

---

## Configuration

Rules are defined in `config/rules.yaml`:

```yaml
tables:
  - name: orders
    path: dbfs:/dq_framework/data/orders
    rules:
      - rule_id: orders_001
        type: not_null
        column: order_id
        severity: critical

      - rule_id: orders_002
        type: range
        column: order_amount
        min: 0
        max: 100000
        severity: warning

      - rule_id: orders_003
        type: referential_integrity
        column: customer_id
        reference_table: dbfs:/dq_framework/data/customers
        reference_column: customer_id
        severity: critical
```

---

## Getting Started

_Setup instructions to be added once environment configuration is finalised._

---

## Audit Log Schema

_Schema to be documented once the evaluator and reporter modules are built._

---

## Dashboard

_Screenshots and description to be added once the dashboard notebook is complete._

---

## Development Stages

- [x] Synthetic data generation
- [ ] YAML config & rule engine
- [ ] Pass/fail evaluator & audit log
- [ ] Quarantine table
- [ ] Dashboard notebook
- [ ] Databricks Workflows orchestration

---

## Author

_Your name / LinkedIn / portfolio link_
