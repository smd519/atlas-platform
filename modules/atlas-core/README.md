# atlas-core

Shared platform primitives used across Atlas modules.

## Exceptions

```python
raise ConfigurationError(
    "Missing required source configuration."
)

raise ConnectorError(
    "Failed to retrieve Environment Canada dataset."
)

raise ValidationError(
    "Dataset schema does not match expected contract."
)
```

## Logging

```python
from atlas_core.logging import configure_logging, get_logger

configure_logging()

logger = get_logger(
    "atlas.ingestion",
    source="environment_organization",
)

logger.info(
    "ingestion_started",
    dataset="weather_observations",
)

logger.warning(
    "quality_check_failed",
    row_count=25,
)
```

## Settings

```python
from atlas_core.settings import settings

print(settings.environment)
print(settings.log_level)
```

Example:

```text
.env.local

ATLAS_LOG_LEVEL=DEBUG
ATLAS_STORAGE_ROOT=data
```

```python
# .env.environment
local
```

`AtlasSettings` automatically loads the correct environment configuration.
