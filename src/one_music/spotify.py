from collections import defaultdict

from spotipy.client import Spotify
from spotipy.oauth2 import SpotifyClientCredentials


def create_authenticator(client_id: str, client_secret: str) -> SpotifyClientCredentials:
    auth_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret,
    )
    return auth_manager


def create_spotify_client(auth_manager: SpotifyClientCredentials) -> Spotify:
    return Spotify(auth_manager=auth_manager)


def get_user_playlists(client: Spotify, user_id: str):
    """Iterate through all playlists of specified user"""
    response = client.user_playlists(user_id)
    while response:
        for playlist in response["items"]:
            yield playlist

        if response["next"]:
            response = client.next(response)
        else:
            response = None


def get_playlist_songs(client: Spotify, playlist_id: str, fields: str = "items(track(id, name, album(release_date), artists(id, name)))"):
    response = client.playlist_items(playlist_id, fields=fields)

    for song in response["items"]:
        song_obj = dict(
            id=song["track"]["id"],
            name=song["track"]["name"],
            release_date=song["track"]["album"]["release_date"],  # TODO include audio features in datamodel
            artists=song["track"]["artists"]
        )

        yield song_obj


def get_audio_features(client: Spotify, song_id: str):
    response = client.audio_features(song_id)

    audio_features_record = defaultdict(lambda: -1)
    for k in ["acousticness", "danceability", "duration_ms", "energy", "speechiness",
              "instrumentalness", "key", "liveness", "mode", "tempo", "valence"]:

        audio_features_record[k] = response[0][k]

    audio_features_record["spotify_id"] = response[0]["id"]

    return audio_features_record
