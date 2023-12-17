from sqlmodel import SQLModel, create_engine

postgresql_url = f"postgresql+psycopg2://user:password@localhost:5432/starvers_db"

engine = create_engine(postgresql_url, echo=True)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)