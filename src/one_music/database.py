from sqlmodel import SQLModel, create_engine


sqlite_url = "sqlite:///database.db"
engine = create_engine(sqlite_url, echo=True)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

