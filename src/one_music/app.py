import hydra

from .models import Playlist
from .database import engine

from one_music.registry import (
    connect_to_db,
    initialize_registry,
    create_table_playlists,
    create_table_songs,
    create_table_songs_playlist,
    insert_playlists,
    insert_songs,
    insert_artists,
    insert_lyrics,
    insert_songs_playlist,
    insert_songs_artists,
    insert_weaviate,
    read_playlists,
    read_songs,
    read_lyrics
)


def poll_spotify(cfg) -> None:
    from one_music.spotify import (
        create_authenticator,
        create_client,
        get_user_playlists,
        filter_playlists,
        get_playlist_songs
    )

    conn = connect_to_db(cfg.registry.connection_string)

    spotify_authenticator = create_authenticator(cfg.spotify.client_id, cfg.spotify.client_secret)
    spotify_client = create_client(auth_manager=spotify_authenticator)

    spotify_playlists = get_user_playlists(client=spotify_client, user_id="spotify")
    top_50_playlists = filter_playlists(
        playlists=spotify_playlists,
        filter_func=lambda p: "Top 50" in p["name"],
        fields=["id", "name", "description"]
    )

    # TODO optimization: multiprocessing, async API calls, batch SQL inserts
    for playlist in top_50_playlists:
        insert_playlists(conn, spotify_id=playlist["id"], name=playlist["name"], description=playlist["description"])

        songs = get_playlist_songs(client=spotify_client, playlist_id=playlist["id"])
        for song in songs:
            insert_songs(conn,
                         spotify_id=song["id"],
                         name=song["name"],
                         release_date=song["release_date"]
                         )
            insert_songs_playlist(conn,
                                  song_spotify_id=song["id"],
                                  playlist_spotify_id=playlist["id"]
                                  )

            for artist in song["artists"]:
                insert_artists(conn,
                              spotify_id=artist["id"],
                              name=artist["name"]
                              )
                insert_songs_artists(conn,
                                     song_spotify_id=song["id"],
                                     artist_spotify_id=artist["id"]
                                     )


def poll_genius(cfg) -> None:
    import time
    import cohere
    from one_music.genius import (
        create_client,
        search_song,
        get_song_lyrics,
        crawl_for_translations,
        detect_lyrics_language,
    )

    conn = connect_to_db(cfg.registry.connection_string)

    cohere_client = cohere.Client(cfg.cohere.client_token)

    genius_client = create_client(cfg.genius.client_token)
    genius_client.remove_section_headers = True

    songs = read_songs(conn)
    # TODO leverage multiprocessing and async API calls
    for song in songs:
        song_spotify_id = song[0]
        song_name = song[2]

        song_genius = search_song(client=genius_client, song_name=song_name)  #, artist_name=artist_name)
        # lyrics = get_song_lyrics(client=genius_client, song_url=song_genius.url)
        # language = detect_lyrics_language(cohere_client, song_genius)  # could be removed

        insert_lyrics(conn,
                      genius_url=song_genius.url,
                      song_spotify_id=song_spotify_id,
                      language=detect_lyrics_language(cohere_client, song_genius),
                      lyrics=get_song_lyrics(client=genius_client, song_url=song_genius.url)
                      )

        for url, language in crawl_for_translations(song_genius.url):
            if url is None:
                break

            insert_lyrics(conn,
                          genius_url=url,
                          song_spotify_id=song_spotify_id,
                          language=language,
                          lyrics=get_song_lyrics(client=genius_client, song_url=url)
                          )

            time.sleep(0.6)

        time.sleep(0.6)


def push_to_weaviate(cfg):
    import time
    from pathlib import Path
    from one_music.weaviate import (
        create_client,
        load_schemas_from_dir,
        push_schemas,
        add_object_to_batch,
        purge_storage
    )

    conn = connect_to_db(cfg.registry.connection_string)

    weaviate_client = create_client(cfg.weaviate.connection_url)

    purge_storage(weaviate_client)

    schema_dir = Path(__file__).with_name("schema")
    schemas = load_schemas_from_dir(schema_dir)
    push_schemas(weaviate_client, schemas)

    weaviate_client.batch.configure(
        batch_size=10,
        dynamic=True
    )

    with weaviate_client.batch as batch:
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

        for lyrics in read_lyrics(conn):
            uuid = add_object_to_batch(batch,
                                       class_name="Lyrics",
                                       data_object={k:v for k, v in lyrics.items() if k in ["genius_url", "language", "lyrics"]}
                                       )
            time.sleep(0.6)

            insert_weaviate(conn,
                            uuid=str(uuid),
                            class_name="Lyrics",
                            primary_key="genius_url",
                            key_value=lyrics["genius_url"]
                            )

        # SECTION 2: add references


@hydra.main(config_name="app.yaml", config_path="../config", version_base="1.2")
def main(cfg) -> None:
    conn = connect_to_db(cfg.registry.connection_string)
    initialize_registry(conn)

    # poll_spotify(cfg)
    # poll_genius(cfg)
    push_to_weaviate(cfg)


if __name__ == "__main__":
    main()
