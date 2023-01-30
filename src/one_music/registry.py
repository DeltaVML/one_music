import sqlite3
from datetime import datetime, timezone


def connect_to_db(connection_string):
    return sqlite3.connect(connection_string)


def create_table_playlists(conn) -> None:
    conn.cursor().execute(
        """
        CREATE TABLE IF NOT EXISTS playlists (
         spotify_id TEXT PRIMARY KEY,
         date_added TEXT,
         name TEXT,
         description TEXT
        )  
        """
    )
    conn.commit()


def create_table_songs(conn) -> None:
    conn.cursor().execute(
        """
        CREATE TABLE IF NOT EXISTS songs (
         spotify_id TEXT PRIMARY KEY,
         date_added TEXT,
         name TEXT,
         release_date TEXT
        )  
        """
    )
    conn.commit()


def create_table_artists(conn) -> None:
    conn.cursor().execute(
        """
        CREATE TABLE IF NOT EXISTS artists (
         spotify_id TEXT PRIMARY KEY,
         name TEXT
        )  
        """
    )
    conn.commit()


def create_table_lyrics(conn) -> None:
    conn.cursor().execute(
        """
        CREATE TABLE IF NOT EXISTS lyrics (
         genius_url TEXT,
         song_spotify_id TEXT PRIMARY KEY,
         date_added TEXT,
         language TEXT,
         lyrics TEXT,
         FOREIGN KEY(song_spotify_id) REFERENCES songs(spotify_id)
        )  
        """
    )
    conn.commit()


def create_table_songs_playlist(conn) -> None:
    conn.cursor().execute(
        """
        CREATE TABLE IF NOT EXISTS songs_playlist (
         song_spotify_id TEXT,
         playlist_spotify_id TEXT,
         FOREIGN KEY(song_spotify_id) REFERENCES songs(spotify_id),
         FOREIGN KEY(playlist_spotify_id) REFERENCES playlists(spotify_id)
        )  
        """
    )
    conn.commit()


def create_table_songs_artists(conn) -> None:
    conn.cursor().execute(
        """
        CREATE TABLE IF NOT EXISTS songs_artists (
         song_spotify_id TEXT,
         artist_spotify_id TEXT,
         FOREIGN KEY(song_spotify_id) REFERENCES songs(spotify_id),
         FOREIGN KEY(artist_spotify_id) REFERENCES artists(spotify_id)
        )  
        """
    )
    conn.commit()


def initialize_registry(conn) -> None:
    create_table_playlists(conn)
    create_table_songs(conn)
    create_table_artists(conn)
    create_table_lyrics(conn)
    create_table_songs_playlist(conn)
    create_table_songs_artists(conn)
    create_weaviate_table(conn)


def insert_playlists(conn, spotify_id, name, description) -> None:
    conn.cursor().execute(
        """
        INSERT OR IGNORE INTO playlists (spotify_id, date_added, name, description)
        VALUES (:spotify_id, CURRENT_TIMESTAMP, :name, :description)
        """,
        {"spotify_id": spotify_id, "name": name, "description": description}
    )
    conn.commit()


def insert_songs(conn, spotify_id, name, release_date) -> None:
    # TODO include other song attributes from spotify
    conn.cursor().execute(
        """
        INSERT OR IGNORE INTO songs (spotify_id, date_added, name, release_date)
        VALUES (:spotify_id, CURRENT_TIMESTAMP, :name, :release_date)
        """,
        {"spotify_id": spotify_id, "name": name, "release_date": release_date}
    )
    conn.commit()


def insert_artists(conn, spotify_id, name) -> None:
    conn.cursor().execute(
        """
        INSERT OR IGNORE INTO artists (spotify_id, name)
        VALUES (:spotify_id, :name)
        """,
        {"spotify_id": spotify_id, "name": name}
    )
    conn.commit()


def insert_lyrics(conn, genius_url, song_spotify_id, language, lyrics) -> None:
    conn.cursor().execute(
        """
        INSERT OR IGNORE INTO lyrics (genius_url, song_spotify_id, date_added, language, lyrics)
        VALUES (:genius_url, :song_spotify_id, CURRENT_TIMESTAMP, :language, :lyrics)
        """,
        {"genius_url": genius_url, "song_spotify_id": song_spotify_id, "language": language, "lyrics": lyrics}
    )
    conn.commit()


def insert_songs_playlist(conn, song_spotify_id, playlist_spotify_id) -> None:
    conn.cursor().execute(
        """
        INSERT OR IGNORE INTO songs_playlist (song_spotify_id, playlist_spotify_id)
        VALUES (:song_spotify_id, :playlist_spotify_id)
        """,
        {"song_spotify_id": song_spotify_id, "playlist_spotify_id": playlist_spotify_id}
    )
    conn.commit()


def insert_songs_artists(conn, song_spotify_id, artist_spotify_id) -> None:
    conn.cursor().execute(
        """
        INSERT OR IGNORE INTO songs_artists (song_spotify_id, artist_spotify_id)
        VALUES (:song_spotify_id, :artist_spotify_id)
        """,
        {"song_spotify_id": song_spotify_id, "artist_spotify_id": artist_spotify_id}
    )
    conn.commit()


def _create_object(keys, values):
    object = dict(zip(keys, values))

    if object.get("date_added"):
        object["date_added"] = str(datetime.strptime(object["date_added"], '%Y-%m-%d %H:%M:%S').astimezone().isoformat())

    elif object.get("release_date"):
        object["release_date"] = str(datetime.strptime(object["release_date"], '%Y-%m-%d').astimezone().isoformat())

    return object


def read_songs(conn) -> list:
    cur = conn.cursor()
    cur.execute("""SELECT * FROM songs""")
    results = cur.fetchall()
    cur.close()
    keys = ("spotify_id", "date_added", "name", "release_date")
    return [_create_object(keys, values) for values in results]


def read_playlists(conn) -> list:
    cur = conn.cursor()
    cur.execute("""SELECT * FROM playlists""")
    results = cur.fetchall()
    cur.close()
    keys = ("spotify_id", "date_added", "name", "description")
    return [_create_object(keys, values) for values in results]


def read_songs_playlist(conn) -> list:
    cur = conn.cursor()
    cur.execute("""SELECT * FROM songs_playlist""")
    results = cur.fetchall()
    cur.close()
    keys = ("song_spotify_id", "playlist_spotify_id")
    return [_create_object(keys, values) for values in results]


def read_lyrics(conn) -> list:
    cur = conn.cursor()
    cur.execute("""SELECT * FROM lyrics""")
    results = cur.fetchall()
    cur.close()
    keys = ("genius_url", "song_spotify_id", "date_added", "language", "lyrics")
    return [_create_object(keys, values) for values in results]


def create_weaviate_table(conn) -> None:
    conn.cursor().execute(
        """
        CREATE TABLE IF NOT EXISTS weaviate (
            uuid TEXT primary key,
            date_added TEXT, 
            class_name TEXT,
            primary_key TEXT,
            key_value TEXT UNIQUE
        )
        """
    )
    conn.commit()


def insert_weaviate(conn, uuid, class_name, primary_key, key_value) -> None:
    conn.cursor().execute(
        """
        INSERT OR IGNORE INTO weaviate (uuid, date_added, class_name, primary_key, key_value)
        VALUES (:uuid, CURRENT_TIMESTAMP, :class_name, :primary_key, :key_value)
        """,
        {"uuid": uuid, "class_name": class_name, "primary_key": primary_key, "key_value": key_value}
    )
    conn.commit()


def get_registry_article(conn):
    cur = conn.cursor()
    cur.execute(
        """     
        SELECT
            rss_feeds.title,
            articles.url,
            articles.title,
            weaviate.yaml.uuid
        FROM weaviate.yaml
        INNER JOIN articles ON weaviate.yaml.url = articles.url
        INNER JOIN rss_feeds ON articles.base_url = rss_feeds.feed_url
        """
    )
    results = cur.fetchall()
    cur.close()
    return results
