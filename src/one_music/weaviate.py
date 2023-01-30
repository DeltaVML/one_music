import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel
from weaviate.client import Client
from weaviate.batch import Batch


class Schema(BaseModel):
    _class: str
    description: str
    vectorIndexType: str
    vectorizer: str
    properties: list[dict]
    moduleConfig: Optional[dict]


def create_client(url: str = "http://localhost:8080") -> Client:
    """Create a weaviate.yaml client using SDK"""
    client = Client(url)   # TODO config DB
    if client.is_live() and client.is_ready():
        return client
    else:
        print("Error creating client")  # TODO implement exception
        raise Exception


def load_schema_file(file_path: str | Path) -> Schema:
    """Load a single schema json file"""
    try:
        with open(file_path) as f:
            return json.load(f)

    except FileNotFoundError:
        print(f"`{file_path}` not found")


def load_schemas_from_dir(schema_dir: str | Path) -> list[Schema]:
    """Load all schema json files found in directory"""
    schema_dir = Path(schema_dir)

    schemas = []
    for file_path in schema_dir.iterdir():
        if file_path.suffix == ".json":
            schema = load_schema_file(file_path)
            schemas.append(schema)

    return schemas


def push_schemas(client: Client, schemas: Schema | list[Schema]) -> None:
    """Push schemas to Weaviate engine"""
    if not isinstance(schemas, list):
        schemas = list(schemas)

    schemas_dict = dict(classes=schemas)
    client.schema.create(schemas_dict)


def purge_storage(client: Client):
    """Remove all schemas, and subsequently delete all indexed objects"""
    client.schema.delete_all()


def add_object_to_batch(batch: Batch, class_name: str, data_object: dict):
    uuid = batch.add_data_object(
        class_name=class_name,
        data_object=data_object
    )
    return uuid
