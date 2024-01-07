from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    environment: str = Field(alias='ENVIRONMENT')
    db_url: str = Field(alias='DB_URL')
    db_echo_sql: bool = Field(alias='DB_ECHO_SQL')
    graph_db_url: str = Field(alias='GRAPH_DB_URL')

    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    def isEnvironment(self, env: str):
        return self.environment == env