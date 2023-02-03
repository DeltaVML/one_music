import pinecone


def initialize_pinecone(api_key: str, environment: str) -> None:
    pinecone.init(api_key, environment=environment)


def get_or_create_index(index_name: str, dimension: int, metric: str) -> pinecone.Index:
    if index_name not in pinecone.list_indexes():

        pinecone.create_index(
            index_name,
            dimension=dimension,
            metric=metric
        )

    return pinecone.Index(index_name)


def delete_index(index_name: str) -> None:
    pinecone.delete_index(index_name)


def query_index(index: pinecone.Index, embedded_query: list[float], top_k: int = 5, **kwargs):
    assert isinstance(embedded_query, list)

    return index.query(embedded_query, top_k=top_k, **kwargs)


def fetch_vectors(index: pinecone.Index, vector_ids: list[str]) -> tuple[list[str], list[list[float]]]:
    response = index.fetch(vector_ids)
    ids = []
    vectors = []
    for obj in response["vectors"].values():
        ids.append(obj["id"])
        vectors.append(obj["values"])

    return ids, vectors
