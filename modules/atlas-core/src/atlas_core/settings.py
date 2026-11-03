from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


ENVIRONMENT = (
    Path(".env.environment").read_text().strip()
    if Path(".env.environment").exists()
    else "development"
)

ENV_FILE = f".env.{ENVIRONMENT}"


class AtlasSettings(BaseSettings):
    environment: str = ENVIRONMENT
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_prefix="ATLAS_",
        extra="ignore",
    )


settings = AtlasSettings()