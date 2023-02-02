from sqlmodel import SQLModel, Session, create_engine
from sqlmodel.main import SQLModelMetaclass


sqlite_url = "sqlite:///database.db"
engine = create_engine(sqlite_url, echo=True)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def get_or_create(session: Session, record: dict, class_: SQLModelMetaclass, primary_key: str):
    # assert class_ in METACLASSES

    obj = session.get(class_, record[primary_key])

    if obj is None:  # noqa
        obj = class_(**record)

    return obj
