from sqlmodel import SQLModel, create_engine
from app.app_config import Settings

engine = create_engine(Settings().db_url, echo=Settings().db_echo_sql)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)