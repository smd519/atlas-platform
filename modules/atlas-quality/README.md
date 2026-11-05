# atlas-quality

`atlas-quality` is the domain-agnostic data quality assessment module for the **Atlas Platform** modular monorepo.

It provides reusable, configurable, and engine-abstracted data quality rules, optional dataset profiling, immutable assessment reports, and execution tracking across datasets at any lifecycle stage (`bronze`, `silver`, `gold`, or in-memory).

---

## 1. Architectural Responsibilities

- **Lifecycle Independence**: Assesses datasets regardless of storage layer (`bronze`, `silver`, `gold`) or in-memory state.
- **Separation of Assessment and Promotion**: Evaluates rules and produces immutable `QualityReport` snapshots. Downstream `atlas-orchestration` consumes these reports to make promotion or quarantine decisions.
- **Engine Abstraction**: Rule implementations in `atlas_quality.rules` operate against `QualityDatasetAdapter` rather than coupling directly to `pandas`, enabling future PySpark or SQL execution engines.
- **Privacy by Default**: `QualityIssue` redacts sample source values by default (`expose_samples=False`) and enforces configurable `sample_limit` bounds.
- **Safe Rule Evaluation**: Configurable consistency and validity rules use structured operators and never invoke `eval()` or arbitrary code execution.

### Dependency Rules

- **Allowed dependencies**: `atlas-core`, `atlas-storage`, `pandas`, `pydantic`
- **Forbidden dependencies**: `atlas-ingestion`, `atlas-processing`, `atlas-orchestration`, `atlas-metadata`, `atlas-spatial`, or any service under `services/`.

---

## 2. Package Structure

```text
modules/atlas-quality/
├── pyproject.toml
├── README.md
├── src/
│   └── atlas_quality/
│       ├── __init__.py
│       ├── py.typed
│       ├── service.py
│       ├── engine.py
│       ├── pipeline.py
│       ├── exceptions.py
│       ├── models/
│       │   ├── __init__.py
│       │   ├── quality_rule.py
│       │   ├── quality_issue.py
│       │   ├── quality_result.py
│       │   └── quality_report.py
│       ├── rules/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── registry.py
│       │   ├── schema.py
│       │   ├── completeness.py
│       │   ├── uniqueness.py
│       │   ├── validity.py
│       │   ├── consistency.py
│       │   └── referential_integrity.py
│       ├── profiling/
│       │   ├── __init__.py
│       │   └── profiler.py
│       └── tracking/
│           ├── __init__.py
│           └── execution_tracker.py
└── tests/
    ├── unit/
    └── integration/
```

---

## 3. Quality Rule Categories & Built-in Rules

| Category | Rule ID | Class | Description |
| :--- | :--- | :--- | :--- |
| `SCHEMA` | `schema.required_columns` | `RequiredColumnsRule` | Verifies required columns exist |
| `SCHEMA` | `schema.unexpected_columns` | `UnexpectedColumnsRule` | Flags columns outside an allowed set |
| `SCHEMA` | `schema.column_types` | `ColumnTypesRule` | Validates logical column types |
| `SCHEMA` | `schema.compatibility` | `SchemaCompatibilityRule` | Validates required columns, types, and extra-column policy |
| `COMPLETENESS` | `completeness.not_null` | `NotNullRule` | Checks non-null completeness on a column |
| `COMPLETENESS` | `completeness.threshold` | `CompletenessThresholdRule` | Enforces minimum non-null percentage |
| `COMPLETENESS` | `completeness.required_fields` | `RequiredFieldsCompletenessRule` | Verifies row-level completeness across required fields |
| `UNIQUENESS` | `uniqueness.unique_column` | `UniqueColumnRule` | Detects duplicate values in a single column |
| `UNIQUENESS` | `uniqueness.composite_key` | `CompositeKeyUniquenessRule` | Detects duplicate composite keys across columns |
| `UNIQUENESS` | `uniqueness.no_duplicate_rows` | `DuplicateRowsRule` | Detects duplicate full rows |
| `VALIDITY` | `validity.allowed_values` | `AllowedValuesRule` | Checks membership in an allowed value set |
| `VALIDITY` | `validity.numeric_range` | `NumericRangeRule` | Checks numeric bounds `[min_value, max_value]` |
| `VALIDITY` | `validity.string_length` | `StringLengthRule` | Checks string length bounds `[min_length, max_length]` |
| `VALIDITY` | `validity.regex` | `RegexMatchRule` | Validates strings against a regular expression |
| `VALIDITY` | `validity.type_or_date` | `DateAndTypeParseRule` | Validates date/datetime/numeric/boolean parseability |
| `CONSISTENCY` | `consistency.column_comparison` | `ColumnComparisonRule` | Compares two columns (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`) |
| `CONSISTENCY` | `consistency.conditional_requirement` | `ConditionalRequirementRule` | Enforces `when` column predicate -> `then` column predicate |
| `CONSISTENCY` | `consistency.expression` | `ArithmeticConsistencyRule` | Safe arithmetic check (`add`, `subtract`, `multiply`, `divide`) |
| `REFERENTIAL_INTEGRITY` | `referential_integrity.foreign_key` | `ForeignKeyRule` | Checks single/composite foreign keys against reference data |

---

## 4. Severity Levels & Assessment Statuses

- **Rule Severities (`QualityRuleSeverity`)**: `INFO`, `WARNING`, `ERROR`, `CRITICAL`
- **Report Statuses (`QualityAssessmentStatus`)**:
  - `PASSED`: All rules passed with no execution errors.
  - `PASSED_WITH_WARNINGS`: Only `INFO` or `WARNING` rules failed; no `ERROR`/`CRITICAL` failures or execution errors.
  - `FAILED`: One or more `ERROR` or `CRITICAL` rules failed (with no execution errors).
  - `ERROR`: One or more rules or pipeline steps encountered an execution error.

---

## 5. Usage Example

```python
from atlas_core import DatasetId
from atlas_quality import (
    QualityAssessmentRequest,
    QualityRuleConfig,
    QualityRuleSeverity,
    QualityService,
)

dataset = [
    {"order_id": "ORD-001", "customer_id": "C-10", "amount": 120.50},
    {"order_id": "ORD-002", "customer_id": "C-20", "amount": 75.00},
]

service = QualityService()
request = QualityAssessmentRequest(
    dataset_id=DatasetId("orders_silver"),
    dataset=dataset,
    enable_profiling=True,
    rules=[
        QualityRuleConfig(
            rule_id="schema.required_columns",
            severity=QualityRuleSeverity.CRITICAL,
            params={"required_columns": ["order_id", "customer_id", "amount"]},
        ),
        QualityRuleConfig(
            rule_id="completeness.not_null",
            severity=QualityRuleSeverity.ERROR,
            params={"column": "customer_id"},
        ),
        QualityRuleConfig(
            rule_id="uniqueness.unique_column",
            severity=QualityRuleSeverity.ERROR,
            params={"column": "order_id"},
        ),
    ],
)

report = service.assess(request)
print(report.overall_status, report.quality_score)
```

---

## 6. Testing

Run unit and integration tests from the repository root:

```bash
pytest modules/atlas-quality/tests -v
```
