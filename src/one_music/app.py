import hydra
from sqlmodel import Session, select

from .models import Playlist, Song, Artist, Lyrics, SongPlaylistLink, SongArtistLink
from .database import engine, create_db_and_tables


def poll_spotify(cfg) -> None:
    from .spotify import (
        create_authenticator,
        create_client,
        get_user_playlists,
        get_playlist_songs
    )

    spotify_authenticator = create_authenticator(cfg.spotify.client_id, cfg.spotify.client_secret)
    spotify_client = create_client(auth_manager=spotify_authenticator)

    spotify_playlists = get_user_playlists(client=spotify_client, user_id="spotify")
    filter_func = lambda p: "Top 50 -" in p["name"]

    # TODO optimization: multiprocessing, async API calls, batch SQL inserts
    with Session(engine) as session:
        for playlist in filter(filter_func, spotify_playlists):
            playlist_sql = Playlist(
                spotify_id=playlist["id"],
                name=playlist["name"],
                description=playlist["description"],
            )
            session.add(playlist_sql)

            songs = get_playlist_songs(client=spotify_client, playlist_id=playlist["id"])
            if songs is None:
                continue

            for song in songs:
                song_sql = Song(
                    spotify_id=song["id"],
                    name=song["name"],
                    playlist=[playlist_sql]
                )
                songplaylistlink_sql = SongPlaylistLink(song=song_sql, playlist=playlist_sql)
                session.add(song_sql)
                session.add(songplaylistlink_sql)

                for artist in song["artists"]:
                    artist_sql = Artist(
                        spotify_id=artist["id"],
                        name=artist["name"],
                        songs=[song_sql]
                    )
                    songartistlink_sql = SongArtistLink(song=song_sql, artist=artist_sql)
                    session.add(artist_sql)
                    session.add(songartistlink_sql)

            session.commit()

        session.commit()


def poll_genius(cfg, start) -> None:
    import time
    import random

    import cohere
    from .genius import (
        create_client,
        search_song,
        crawl_for_translations,
        detect_lyrics_language,
        get_song_lyrics,
        save_lyrics_to_file
    )

    cohere_client = cohere.Client(cfg.cohere.client_token)
    genius_client = create_client(cfg.genius.client_token)

    with Session(engine) as session:
        query = select(SongArtistLink).join(Song).join(Artist).where(Song.id > start)
        results = session.exec(query)

        keys = set()
        unique_songs = []
        for r in results:
            if r.song.spotify_id in keys:
                continue
            else:
                keys.add(r.song.spotify_id)
                unique_songs.append(
                    dict(
                        song_id=r.song.id,
                        song_spotify_id=r.song.spotify_id,
                        song_name=r.song.name,
                        artist_name=r.artist.name,
                    )
                )

    # TODO leverage multiprocessing and async API calls
    # for r in results:
    for s in unique_songs:
        # NOTE Genius could return translations as primary result
        # song_genius = search_song(client=genius_client, song_name=r.song.name, artist_name=r.artist.name)
        song_genius = search_song(client=genius_client, song_name=s["song_name"], artist_name=s["artist_name"])

        if song_genius:
            if song_genius.lyrics:

                lyrics_snippet = song_genius.lyrics[300:500]
                language = detect_lyrics_language(cohere_client, lyrics_snippet)

                file_name = f'{s["song_id"]}_{language}.txt'
                save_lyrics_to_file(lyrics=song_genius.lyrics, file_name=file_name, save_dir=cfg.genius.save_dir)

                main_lyrics_sql = Lyrics(
                    genius_url=song_genius.url,
                    song_id=s["song_id"],
                    language=language,
                    is_downloaded=True
                )
                session.add(main_lyrics_sql)

            for translation_url, language in crawl_for_translations(song_genius.url):
                if translation_url is None:
                    break

                translation_lyrics = get_song_lyrics(client=genius_client, song_url=translation_url)
                if translation_lyrics:
                    translation_lyrics_snippet = translation_lyrics[300:500]
                    translation_language = detect_lyrics_language(cohere_client, translation_lyrics_snippet)

                    file_name = f'{s["song_id"]}_{translation_language}.txt'
                    save_lyrics_to_file(translation_lyrics, file_name=file_name, save_dir=cfg.genius.save_dir)

                    translation_lyrics_sql = Lyrics(
                        genius_url=translation_url,
                        song_id=s["song_id"],
                        language=translation_language,  # cohere enforces coherent formatting vs. scraper
                        is_downloaded=True
                    )
                    session.add(translation_lyrics_sql)

                time.sleep(60 + random.randint(0, 120))  # sleep for cohere API calls

        session.commit()

    time.sleep(60 + random.randint(0, 120))  # sleep for cohere API calls


def push_to_weaviate(cfg):
    import time
    from pathlib import Path
    from .weaviate import (
        create_client,
        load_schemas_from_dir,
        push_schemas,
        add_object_to_batch,
        purge_storage
    )

    def _load_lyrics_file(file_path):
        with open(file_path, mode="r", encoding="utf-8") as f:
            return f.read()

    def strip_headers():
        NotImplementedError

    weaviate_client = create_client(cfg.weaviate.connection_url)

    purge_storage(weaviate_client)

    base_dir = Path(__file__).parent
    schemas = load_schemas_from_dir(base_dir.joinpath("schema"))
    push_schemas(weaviate_client, schemas)

    weaviate_client.batch.configure(batch_size=100)

    count = 0
        # SECTION 1: add objects
        # for playlist in read_playlists(conn):
        #     uuid = add_object_to_batch(batch,
        #                                class_name="Playlist",
        #                                data_object=playlist
        #                                )
        #     time.sleep(0.6)
        #
        #     insert_weaviate(conn,
        #                     uuid=str(uuid),
        #                     class_name="Playlist",
        #                     primary_key="spotify_id",
        #                     key_value=playlist["spotify_id"]
        #                     )

        # for song in read_songs(conn):
        #     uuid = add_object_to_batch(batch,
        #                                class_name="Song",
        #                                data_object=song
        #                                )
        #     time.sleep(0.6)
        #
        #     insert_weaviate(conn,
        #                     uuid=str(uuid),
        #                     class_name="Song",
        #                     primary_key="spotify_id",
        #                     key_value=song["spotify_id"]
        #                     )

    with Session(engine) as session:
        query = select(Lyrics)
        results = session.exec(query)

        for r in results:
            file_name = f"{r.song_id}_{r.language}.txt"

            try:
                lyrics_txt = _load_lyrics_file(base_dir.parent.joinpath("data", file_name))
            except FileNotFoundError:
                continue

            uuid = add_object_to_batch(weaviate_client.batch,
                                       class_name="Lyrics",
                                       data_object=dict(genius_url=r.genius_url, language=r.language, lyrics=lyrics_txt)
                                       )

            count += 1
            if count % 100 == 0:
                weaviate_client.batch.flush()
                time.sleep(62)

        # SECTION 2: add references


@hydra.main(config_name="app.yaml", config_path="../config", version_base="1.2")
def main(cfg) -> None:
    import time
    from pathlib import Path

    def _find_start():
        data_dir = Path("C:/.coding/cohere_hackaton/src/data")
        start = 0
        for f in data_dir.iterdir():
            song_id = int(f.name.split("_")[0])
            if start < song_id:
                start = song_id

        return start

    waits = [600, 1800, 1800, 3600, 1800, 600, 1800, 3600, 1800, 2700]
    for w in waits:
        try:
            poll_genius(cfg, start=_find_start())
        except:
            time.sleep(w)

    # create_db_and_tables()
    # poll_spotify(cfg)
    # poll_genius(cfg)
    # push_to_weaviate(cfg)


if __name__ == "__main__":
    main()
