# Atlas Platform - Getting Started

## Overview

Atlas is a modular Python data platform for ingesting, transforming, storing, and validating data using a Medallion architecture.

### Data Lifecycle

- **Bronze**: Raw ingested data stored in Parquet format.
- **Silver**: Transformed and curated data.
- **Gold**: Future analytical-ready datasets.

---

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Git

---

## Project Setup

Clone the repository and install dependencies:

```bash
git clone <repository-url>
cd atlas-platform
uv sync
```

---

## Customer Workspace

Atlas keeps customer data and configuration outside the platform repository.

Create a workspace with the following structure:

```text
ATLAS-WORKSPACE/
├── drop/
│   └── customers.csv
├── jobs/
│   └── customers.yml
├── workflows/
│   └── customers-pipeline.yml
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── schedules/
├── logs/
└── metadata/
```

Place source files in `drop/`.

---

## Configure Your Pipeline

Atlas uses two YAML configuration files.

### Workflow Definition

`workflows/customers-pipeline.yml`

Defines:

- Pipeline steps
- Dependencies
- Parameters
- Transformations
- Quality rules

### Job Definition

`jobs/customers.yml`

Defines:

- Job identifier
- Referenced workflow
- Runtime parameters

The Job YAML references the Workflow YAML. The runner loads both and constructs the workflow definition at runtime.

---

## Validate Your Configuration

Before execution, validate the job and workflow configurations:

```bash
uv run atlas-runner \
  --workspace D:\WORK-SPACE\ATLAS-WORKSPACE \
  --job jobs/customers.yml \
  --validate-only
```

Validation checks:

- Configuration structure
- Workspace paths
- Workflow dependencies

No data is processed during validation.

---

## Run Your Pipeline

Execute the job manually:

```bash
uv run atlas-runner \
  --workspace D:\WORK-SPACE\ATLAS-WORKSPACE \
  --job jobs/customers.yml
```

---

## Execution Flow

```text
Customer Workspace
       |
       v
    Job YAML
       |
       v
  Workflow YAML
       |
       v
   Atlas Runner
       |
       v
Atlas Orchestration
       |
       v
  Platform Modules
       |
       v
 Bronze -> Silver -> Gold
```

The runner handles configuration loading, dependency initialization, and execution reporting.

Orchestration manages workflow execution and step dependencies.

---

## Expected Output

```text
ATLAS-WORKSPACE/
├── drop/
│   └── customers.csv
├── jobs/
│   └── customers.yml
├── workflows/
│   └── customers-pipeline.yml
└── data/
    ├── bronze/
    │   └── customers.parquet
    └── silver/
        └── customers.parquet
```

---

## Execution Results

Example successful execution:

```text
========== ATLAS EXECUTION ==========
Job: customers
Workflow: customers_pipeline
Execution: <execution-id>
Status: SUCCEEDED

Steps:
  ingest:   SUCCEEDED
  process:  SUCCEEDED
  quality:  SUCCEEDED
====================================
```

If a workflow step fails, Atlas reports the execution status and the affected step.

---

## Current Platform Scope

### Supported

- YAML-based job definitions
- YAML-based workflow definitions
- Configuration validation
- Workspace-relative path resolution
- Local file ingestion
- CSV datasets
- Parquet storage
- Data transformations
- Data quality validation
- Sequential workflow execution
- Manual execution through Atlas Runner

### Planned

- Scheduled execution through `atlas-scheduler`
- API-triggered execution through `atlas-api`
- Cloud storage
- Distributed execution
- Gold data products