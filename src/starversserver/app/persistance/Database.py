from sqlmodel import SQLModel, create_engine, Session
from app.AppConfig import Settings

engine = create_engine(Settings().db_url, echo=Settings().db_echo_sql)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session
