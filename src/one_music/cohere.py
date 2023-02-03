import time
from requests.exceptions import RetryError

import cohere


def wait_retry(wait_time, exceptions):
    def decorator(func):
        def newfunc(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions:
                time.sleep(wait_time)
            return func(*args, **kwargs)
        return newfunc
    return decorator


def create_cohere_client(api_key: str) -> cohere.Client:
    return cohere.Client(api_key)


@wait_retry(wait_time=60, exceptions=(RetryError,))
def detect_lyrics_language(cohere_client: cohere.Client, lyrics_snippet) -> tuple[str, str]:
    response = cohere_client.detect_language(texts=[lyrics_snippet])
    return response.results[0].language_name, response.results[0].language_code


@wait_retry(wait_time=60, exceptions=(RetryError,))
def embed_texts(cohere_client: cohere.Client, texts=[]):
    embeds = cohere_client.embed(
        texts=texts,
        model='multilingual-22-12',
        truncate="END"
    ).embeddings
    return embeds


