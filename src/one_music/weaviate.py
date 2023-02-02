import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel
from weaviate.client import Client
from weaviate.util import generate_uuid5


class Schema(BaseModel):
    _class: str
    description: str
    vectorIndexType: str
    vectorizer: str
    properties: list[dict]
    moduleConfig: Optional[dict]


def create_weaviate_client(url: str, headers: dict = None) -> Client:
    """Create a weaviate client using SDK"""
    headers = {} if headers is None else headers
    client = Client(url, additional_headers=headers)   # TODO config DB
    if client.is_live() and client.is_ready():
        return client
    else:
        print("Error creating client")  # TODO implement exception
        raise Exception


def initialize_weaviate(client: Client, schema_dir: str) -> None:
    if client.schema.contains() is False:
        schemas = load_schemas_from_dir(schema_dir)
        push_schemas(client, schemas)


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


from weaviate import Client
import time


def configure_batch(client: Client, batch_size: int, batch_target_rate: float):
    """
    func to respect Cohere rate limit; ref: https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-cohere#cohere-rate-limits
    Configure the weaviate client's batch so it creates objects at `batch_target_rate`.

    Parameters
    ----------
    client : Client
        The Weaviate client instance.
    batch_size : int
        The batch size.
    batch_target_rate : int
        The batch target rate as # of objects per second.
    """

    def callback(batch_results: dict) -> None:
        time_took_to_create_batch = batch_size * (client.batch.creation_time/20)
        time.sleep(
            round(max(batch_size/batch_target_rate - time_took_to_create_batch + 1, 0))
        )

    client.batch.configure(
        batch_size=batch_size,
        timeout_retries=5,
        callback=callback,
    )


def get_or_add_to_batch(client: Client, data_object: dict, class_name: str, primary_key: str) -> str:
    query = (
        client.query.get(class_name=class_name, properties=primary_key)
                    .with_additional(properties="id")
                    .with_where(
                            {"path": [primary_key],
                             "operator": "Equal",
                             "valueString": data_object[primary_key],
                             }
                        )
                    .with_limit(1)
    )
    result = query.do()

    if not result["data"]["Get"][class_name]:
        uuid = generate_uuid5(data_object, class_name)
        client.batch.add_data_object(
            class_name=class_name,
            data_object=data_object,
            uuid=uuid,
        )
    else:
        uuid = result["data"]["Get"][class_name][0]["_additional"]["id"]

    return uuid
