# Atlas Platform Roadmap

Delivery matrix mapping platform capabilities (rows) to the modules that own them (columns).
It is derived **strictly from the module `README.md` files** under `modules/`; where a README is
silent, the cell is not marked complete.

**Legend:** ✅ complete (documented) · 🟡 implemented but not documented in that module's README · ⬜ pending / not documented · — not applicable

## Phase 1 — Foundation

| Capability | core | storage | connectors | ingestion | processing | quality | metadata | orchestration | spatial |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Shared models, contracts, exceptions, settings, logging | ✅ | — | — | — | — | — | — | — | — |
| Format-agnostic storage (`FormatHandler`/`FormatRegistry`, `StorageProvider` backends) | — | ✅ | — | — | — | — | — | — | — |
| Source acquisition: local files, REST, atomic HTTP streaming, parsers, auth | — | — | ✅ | — | — | — | — | — | — |
| Ingestion lifecycle, staging and cleanup, Bronze loading | — | — | — | ✅ | — | — | — | — | — |
| Transformations and curated (Silver) persistence | — | — | — | — | ✅ | — | — | — | — |
| Quality rules, engine, profiling, reports | — | — | — | — | — | ✅ | — | — | — |
| Dataset catalog, schema registry, URN model, artifact provenance | — | — | — | — | — | — | ✅ | — | — |
| Workflow domain model, DAG validation, sequential engine, retries/timeouts, SQLite tracking | — | — | — | — | — | — | — | ✅ | — |
| Module adapters (ingestion / processing / quality executors) | — | — | — | — | — | — | — | ✅ | — |
| Spatial capabilities | — | — | — | — | — | — | — | — | ⬜ |

## Phase 2 — Metadata Integration

| Capability | core | storage | connectors | ingestion | processing | quality | metadata | orchestration | spatial |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `MetadataProvider` contract and canonical URN utilities | ✅ | — | — | — | — | — | — | — | — |
| `MetadataService` implements `MetadataProvider` (no reverse dependency) | — | — | — | — | — | — | ✅ | — | — |
| Ingestion registers Bronze dataset and artifact (with `execution_id`) | — | — | — | ✅ | — | — | ✅ | — | — |
| Processing registers dataset + artifact, normalized/deduplicated lineage (`upstream_urns`) | — | — | — | — | ✅ | — | — | — | — |
| Artifact history preserved across executions; dataset record updated in place | — | — | — | — | ✅ | — | ✅ | — | — |
| Quality results published to catalog (`latest_quality_assessment`, bounded history) | — | — | — | — | — | ✅ | — | — | — |
| Orchestration propagates a `MetadataProvider` to step executors | — | — | — | — | — | — | — | ⬜ | — |

## Notes

- **Ingestion & Quality (✅):** Phase 2 `MetadataProvider` auto-registration for Bronze datasets/artifacts and bounded assessment history publishing under `custom_properties` are documented in `atlas-ingestion` and `atlas-quality` READMEs.
- **Orchestration (⬜):** the README documents Phase 1 (domain foundation) and Phase 2 (sequential execution)
  of that module, and does not mention metadata. Its "Phase" numbering is module-local and distinct from this roadmap.
- **Spatial (⬜):** the README contains only a title.
- Storage persistence and catalog registration in processing are documented as **not transactional**.
- Platform-level planned items from the onboarding guide: scheduler (`atlas-scheduler`), API (`atlas-api`),
  cloud storage, distributed execution, and Gold data products.

See also the long-range phase outline in `docs/architecture/roadmap.md`.
