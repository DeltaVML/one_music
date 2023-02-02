import hydra
from sqlmodel import Session, select

from ..models import Playlist, Song, Artist, Lyrics, AudioFeatures
from ..database import engine, create_db_and_tables, get_or_create


def poll_spotify(cfg) -> None:
    from ..spotify import (
        create_authenticator,
        create_spotify_client,
        get_user_playlists,
        get_playlist_songs
    )

    spotify_authenticator = create_authenticator(cfg.spotify.client_id, cfg.spotify.client_secret)
    spotify_client = create_spotify_client(auth_manager=spotify_authenticator)

    spotify_playlists = get_user_playlists(client=spotify_client, user_id="spotify")
    filter_func = lambda p: "Top 50 -" in p["name"]

    # TODO optimization: multiprocessing, async API calls, batch SQL inserts
    with Session(engine) as session:
        for playlist in filter(filter_func, spotify_playlists):
            playlist_record = dict(
                spotify_id=playlist["id"],
                name=playlist["name"],
                description=playlist["description"],
            )
            playlist_obj = get_or_create(session, playlist_record, Playlist, "spotify_id")
            session.add(playlist_obj)

            songs = get_playlist_songs(client=spotify_client, playlist_id=playlist["id"])
            if songs is None:
                continue

            for song in songs:
                song_record = dict(
                    spotify_id=song["id"],
                    name=song["name"],
                )
                song_obj = get_or_create(session, song_record, Song, "spotify_id")
                song_obj.playlists.append(playlist_obj)
                session.add(song_obj)

                for artist in song["artists"]:
                    artist_record = dict(
                        spotify_id=artist["id"],
                        name=artist["name"],
                    )
                    artist_obj = get_or_create(session, artist_record, Artist, "spotify_id")
                    artist_obj.songs.append(song_obj)
                    session.add(artist_obj)

            session.commit()


def poll_audio_features(cfg):
    from ..spotify import (
        create_authenticator,
        create_spotify_client,
        get_audio_features
    )

    spotify_authenticator = create_authenticator(cfg.spotify.client_id, cfg.spotify.client_secret)
    spotify_client = create_spotify_client(auth_manager=spotify_authenticator)

    with Session(engine) as session:
        query = select(Song)
        results = session.exec(query)

        for song in results:
            audio_features_record = get_audio_features(spotify_client, song_id=song.spotify_id)
            audio_features_obj = get_or_create(session, audio_features_record, AudioFeatures, "spotify_id")
            session.add(audio_features_obj)
            session.commit()


def poll_genius(cfg) -> None:
    import time
    import random

    from ..genius import (
        create_genius_client,
        search_song,
        crawl_for_translations,
        get_song_lyrics,
        generate_file_name,
        save_lyrics_to_file,
    )
    from ..cohere import (
        create_cohere_client,
        detect_lyrics_language,
    )

    cohere_client = create_cohere_client(cfg.cohere.api_key)
    genius_client = create_genius_client(cfg.genius.client_token)

    with Session(engine) as session:
        query = select(Song, Lyrics).join(Lyrics, isouter=True)
        results = session.exec(query)

        for song, lyrics in results:
            if lyrics is not None:
                continue
            # NOTE Genius could return translations as primary result
            song_genius = search_song(client=genius_client, song_name=song.name, artist_name=song.artists[0].name)

            if song_genius is None:  # couldn't find a result for query
                continue
            elif song_genius.lyrics is None:  # some song page are blank
                continue

            lyrics_snippet = song_genius.lyrics[200:]  # selecting end of text because beginning has variable headers
            detected_language_name, detected_language_code = detect_lyrics_language(cohere_client, lyrics_snippet)

            file_name = generate_file_name(song_genius.url)
            save_lyrics_to_file(lyrics=song_genius.lyrics, file_name=file_name, save_dir=cfg.genius.save_dir)

            lyrics_record = dict(
                genius_url=song_genius.url,
                song_spotify_id=song.spotify_id,
                language=detected_language_code,
                file_name=file_name,
            )
            lyrics_obj = get_or_create(session, lyrics_record, Lyrics, "song_spotify_id")
            session.add(lyrics_obj)

            for translation_url, scraped_language in crawl_for_translations(song_genius.url):
                if translation_url is None:  # exhaust crawling results
                    break

                translation_lyrics = get_song_lyrics(client=genius_client, song_url=translation_url)
                if translation_lyrics is None:  # some song page are blank
                    continue

                lyrics_snippet = translation_lyrics[:200]
                detected_language_name, detected_language_code = detect_lyrics_language(cohere_client, lyrics_snippet)

                if scraped_language in ["Romanization", "romanization"]:
                    detected_language_code += "_rom"

                file_name = generate_file_name(song_genius.url)
                save_lyrics_to_file(translation_lyrics, file_name=file_name, save_dir=cfg.genius.save_dir)

                lyrics_record = dict(
                    genius_url=translation_url,
                    song_spotify_id=song.spotify_id,
                    language=detected_language_code,
                    file_name=file_name,
                )
                lyrics_obj = get_or_create(session, lyrics_record, Lyrics, "song_spotify_id")
                session.add(lyrics_obj)

                time.sleep(15 + random.randint(15, 30))  # sleep for Genius API calls

            session.commit()


def push_to_weaviate(cfg):
    from pathlib import Path

    from ..genius import (
        parse_lyrics
    )

    from ..weaviate import (
        create_weaviate_client,
        initialize_weaviate,
        purge_storage,
        configure_batch,
        get_or_add_to_batch,
    )

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
            # Skip song objects already embeded
            if weaviate_client.data_object.get_by_id(song_uuid, class_name="Song"):
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


@hydra.main(config_name="app.yaml", config_path="../../config", version_base="1.2")
def main(cfg) -> None:
    # create_db_and_tables()
    # poll_spotify(cfg)
    # poll_audio_features(cfg)
    # poll_genius(cfg)
    push_to_weaviate(cfg)


if __name__ == "__main__":
    main()
