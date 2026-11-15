# atlas-runner

Generic YAML-driven execution entry point for Atlas Platform workflows.

## Overview

`atlas-runner` acts as the platform's composition root. It loads customer-defined Job and Workflow YAML configurations from an external workspace, applies job-level parameter overrides, and resolves relative paths against the customer workspace. 

Before execution, it initializes and injects all platform dependencies (`atlas-ingestion`, `atlas-processing`, `atlas-quality`, `atlas-storage`, `atlas-connectors`, and `atlas-metadata`). Notably, it configures a single shared `MetadataProvider` across the data plane to ensure that dataset lineage, artifact provenance, and quality assessments are automatically cataloged during the run. It then executes the fully-wired workflow through `atlas-orchestration`.


## CLI Usage

```powershell
uv run atlas-runner --workspace D:\WORK-SPACE\ATLAS-WORKSPACE\customer_001 --job morning_batch
```

## Parameter Merge Strategy

- Workflow parameters provide default values.
- Job parameters override matching top-level parameters.
- Unspecified workflow parameters remain unchanged.
- Nested dictionaries are replaced at the top level (no unrestricted recursive merging).

