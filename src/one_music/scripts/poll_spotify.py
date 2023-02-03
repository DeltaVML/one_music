import hydra
from sqlmodel import Session

from ..models import Playlist, Song, Artist, AudioFeatures
from ..database import engine, create_db_and_tables, get_or_create

from ..spotify import (
    create_authenticator,
    create_spotify_client,
    get_user_playlists,
    get_playlist_songs,
    get_audio_features
)


@hydra.main(config_name="spotify.yaml", config_path="../../config", version_base="1.2")
def poll_spotify(cfg) -> None:

    spotify_authenticator = create_authenticator(cfg.client_id, cfg.client_secret)
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

                audio_features_record = get_audio_features(spotify_client, song_id=song["id"])
                audio_features_obj = get_or_create(session, audio_features_record, AudioFeatures, "spotify_id")
                session.add(audio_features_obj)

                for artist in song["artists"]:
                    artist_record = dict(
                        spotify_id=artist["id"],
                        name=artist["name"],
                    )
                    artist_obj = get_or_create(session, artist_record, Artist, "spotify_id")
                    artist_obj.songs.append(song_obj)
                    session.add(artist_obj)

            session.commit()


if __name__ == "__main__":
    create_db_and_tables()
    poll_spotify()
