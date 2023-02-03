from pathlib import Path

import hydra
from sqlmodel import Session, select

from ..models import Lyrics
from ..database import engine

from ..genius import parse_lyrics
from ..cohere import create_cohere_client, embed_texts
from ..pinecone import initialize_pinecone, get_or_create_index


@hydra.main(config_name="app.yaml", config_path="../../config", version_base="1.2")
def push_to_pinecone(cfg):

    cohere_client = create_cohere_client(cfg.cohere.api_key)

    initialize_pinecone(cfg.pinecone.api_key, cfg.pinecone.environment)
    index = get_or_create_index(cfg.pinecone.index_name, dimension=cfg.pinecone.dimension, metric="cosine")

    with Session(engine) as session:
        query = select(Lyrics)
        results = session.exec(query)

        # TODO async calls to the two APIs
        ids = []
        metadata = []
        lyrics_embeddings = []
        for lyrics in results:
            file_path = Path(cfg.pinecone.data_dir).joinpath(lyrics.file_name)
            try:
                with open(file_path, mode="r", encoding="utf-8") as f:
                    lyrics_txt = parse_lyrics(f.read())
            except FileNotFoundError:
                print("FileNotFoundError:", file_path)
                continue

            embedding = embed_texts(cohere_client, texts=[lyrics_txt])
            lyrics_embeddings.append(embedding)

            ids.append(str(lyrics.file_name.split(".")[0]))
            metadata.append(
                dict(
                    language=lyrics.language,
                    song_spotify_id=lyrics.song_spotify_id
                )
            )

    to_upsert = list(zip(ids, lyrics_embeddings, metadata))

    for i in range(0, len(ids), cfg.pinecone.batch_size):
        i_end = min(i+cfg.pinecone.batch_size, len(ids))
        index.upsert(vectors=to_upsert[i:i_end])


if __name__ == "__main__":
    push_to_pinecone()
