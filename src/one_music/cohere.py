import cohere


def create_cohere_client(client_token: str) -> cohere.Client:
    return cohere.Client(client_token)


def detect_lyrics_language(cohere_client: cohere.Client, lyrics_snippet) -> tuple[str, str]:
    response = cohere_client.detect_language(texts=[lyrics_snippet])
    return response.results[0].language_name, response.results[0].language_code
