from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    database_url: str = "postgres://user:pass@storage:5432/db"
    workers: int = 4
    batch_size: int = 200
    poll_interval_ms: int = 50
    stuck_processing_sec: int = 300
