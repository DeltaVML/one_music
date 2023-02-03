from pathlib import Path

import hydra
from sqlmodel import Session, select

from ..models import Song
from ..database import engine

from ..genius import (
    parse_lyrics
)

from ..weaviate import (
    create_weaviate_client,
    initialize_weaviate,
    configure_batch,
    get_or_add_to_batch,
)


@hydra.main(config_name="app.yaml", config_path="../../config", version_base="1.2")
def push_to_weaviate(cfg):
    weaviate_client = create_weaviate_client(cfg.weaviate.connection_url, headers={"X-Cohere-Api-Key": cfg.cohere.api_key})
    initialize_weaviate(weaviate_client, schema_dir=cfg.weaviate.schema_dir)
    configure_batch(weaviate_client, batch_size=20, batch_target_rate=1.6)

    with Session(engine) as session:
        query = select(Song)
        results = session.exec(query)

        for song in results:
            song_obj = dict(
                spotify_id=song.spotify_id,
                name=song.name,
            )
            song_uuid = get_or_add_to_batch(weaviate_client, data_object=song_obj, class_name="Song", primary_key="spotify_id")

            if weaviate_client.data_object.get_by_id(song_uuid, class_name="Song"):  # Skip song objects already embeded
                continue

            audio_features = song.audio_features[0]
            audio_features_obj = dict(
                spotify_id=audio_features.spotify_id,
                acousticness=audio_features.acousticness,
                danceability=audio_features.danceability,
                duration_ms=audio_features.duration_ms,
                energy=audio_features.energy,
                instrumentalness=audio_features.instrumentalness,
                key=audio_features.key,
                liveness=audio_features.liveness,
                mode=audio_features.mode,
                speechiness=audio_features.speechiness,
                tempo=audio_features.tempo,
                valence=audio_features.valence,
            )

            audio_features_uuid = get_or_add_to_batch(weaviate_client, data_object=audio_features_obj, class_name="AudioFeatures", primary_key="spotify_id")

            weaviate_client.batch.add_reference(
                from_object_uuid=song_uuid,
                from_property_name="has_audio_features",
                to_object_uuid=audio_features_uuid,
                from_object_class_name="Song",
                to_object_class_name="AudioFeatures",
            )
            weaviate_client.batch.add_reference(
                from_object_uuid=audio_features_uuid,
                from_property_name="from_song",
                to_object_uuid=song_uuid,
                from_object_class_name="AudioFeatures",
                to_object_class_name="Song",
            )

            for playlist in song.playlists:
                playlist_obj = dict(
                    spotify_id=playlist.spotify_id,
                    name=playlist.name,
                    description=playlist.description,
                    created_at=str(playlist.created_at.astimezone().isoformat())
                )
                playlist_uuid = get_or_add_to_batch(weaviate_client, data_object=playlist_obj, class_name="Playlist", primary_key="spotify_id")

                weaviate_client.batch.add_reference(
                    from_object_uuid=song_uuid,
                    from_property_name="from_playlists",
                    to_object_uuid=playlist_uuid,
                    from_object_class_name="Song",
                    to_object_class_name="Playlist",
                )
                weaviate_client.batch.add_reference(
                    from_object_uuid=playlist_uuid,
                    from_property_name="has_songs",
                    to_object_uuid=song_uuid,
                    from_object_class_name="Playlist",
                    to_object_class_name="Song",
                )

            for lyrics in song.lyrics:
                file_path = Path(cfg.weaviate.data_dir).joinpath(lyrics.file_name)
                try:
                    with open(file_path, mode="r", encoding="utf-8") as f:
                        lyrics_txt = f.read()
                except FileNotFoundError:
                    print("FileNotFoundError:", file_path)
                    continue

                lyrics_txt = parse_lyrics(lyrics_txt, replace_headers="--")

                lyrics_obj = dict(
                    genius_url=lyrics.genius_url,
                    language=lyrics.language,
                    lyrics=lyrics_txt
                )
                lyrics_uuid = get_or_add_to_batch(weaviate_client, data_object=lyrics_obj, class_name="Lyrics", primary_key="genius_url")

                weaviate_client.batch.add_reference(
                    from_object_uuid=song_uuid,
                    from_property_name="has_lyrics",
                    to_object_uuid=lyrics_uuid,
                    from_object_class_name="Song",
                    to_object_class_name="Lyrics",
                )
                weaviate_client.batch.add_reference(
                    from_object_uuid=lyrics_uuid,
                    from_property_name="from_song",
                    to_object_uuid=song_uuid,
                    from_object_class_name="Lyrics",
                    to_object_class_name="Song",
                )

            # ensure pushing objects before moving to the next song
            weaviate_client.batch.create_objects()
            weaviate_client.batch.create_references()


if __name__ == "__main__":
    push_to_weaviate()
