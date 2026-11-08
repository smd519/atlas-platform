# atlas-runner

Generic YAML-driven execution entry point for Atlas Platform workflows.

## Overview

`atlas-runner` loads customer-defined Job and Workflow YAML configurations from an external workspace, applies job-level parameter overrides, resolves relative paths against the customer workspace, initializes and injects platform dependencies (`atlas-ingestion`, `atlas-processing`, `atlas-quality`, `atlas-storage`, `atlas-connectors`), and executes the workflow through `atlas-orchestration`.

## CLI Usage

```powershell
uv run atlas-runner --workspace D:\WORK-SPACE\ATLAS-WORKSPACE --job jobs/corporations.yml
```

## Parameter Merge Strategy

- Workflow parameters provide default values.
- Job parameters override matching top-level parameters.
- Unspecified workflow parameters remain unchanged.
- Nested dictionaries are replaced at the top level (no unrestricted recursive merging).

