from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    environment: str = Field(alias='ENVIRONMENT')
    db_url: str = Field(alias='MANAGEMENT_DB_URL')
    db_echo_sql: bool = Field(alias='DB_ECHO_SQL')
    graph_db_url: str = Field(alias='GRAPH_DB_URL')
    graph_db_url_get_endpoint: str = Field(alias='GRAPH_DB_URL_GET')
    graph_db_url_post_endpoint: str = Field(alias='GRAPH_DB_URL_POST')
    evaluation_mode: bool = Field(alias="EVALUATION_MODE", default=False)

    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    def isEnvironment(self, env: str):
        return self.environment == env