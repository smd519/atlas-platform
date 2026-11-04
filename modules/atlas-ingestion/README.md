# atlas-ingestion


| Component | Responsibility |
| --- | --- |
| `service.py` | Public ingestion service and module entry point. |
| `pipeline.py` | Coordinates extraction, loading, and tracking activities. |
| `lifecycle.py` | Manages ingestion lifecycle stages and transitions. |
| `exceptions.py` | Defines ingestion-specific exceptions. |
| `models/ingestion_request.py` | Defines ingestion execution requests and parameters. |
| `models/ingestion_result.py` | Defines ingestion results, outcomes, and output references. |
| `extraction/extractor.py` | Extracts data through registered connectors. |
| `loading/loader.py` | Persists extracted data through storage interfaces. |
| `tracking/execution_tracker.py` | Tracks ingestion execution state and outcomes. |
