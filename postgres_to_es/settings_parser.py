from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseSettings

load_dotenv()


class PostgresSettings(BaseSettings):
    """Schema to get .env variables for Postgres"""

    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: Optional[str] = "localhost"
    DB_PORT: Optional[int] = 5432
    DB_OPTIONS: str

    class Config:
        env_file = ".env"


class ElasticSearchSettings(BaseSettings):
    """Schema to get .env variables for ElasticSearch"""

    ETL_HOST: str = "localhost"
    ETL_PORT: Optional[int] = 9200
    ETL_FILE_PATH: str

    class Config:
        env_file = ".env"


class ApplicationSettings(BaseSettings):
    """Schema to get .env variables which will be use in Application"""

    LIMIT: Optional[int] = 100
    FETCH_DELAY: float
    STATE_FIELD: str
    STATE_FILE_NAME: str

    class Config:
        env_file = ".env"


pg_data = PostgresSettings()
es_data = ElasticSearchSettings()
app_data = ApplicationSettings()
