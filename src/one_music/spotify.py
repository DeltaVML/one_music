from typing import Callable

from spotipy.client import Spotify
from spotipy.oauth2 import SpotifyClientCredentials


def create_authenticator(client_id: str, client_secret: str) -> SpotifyClientCredentials:
    auth_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret,
    )
    return auth_manager


def create_client(auth_manager: SpotifyClientCredentials) -> Spotify:
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


def filter_playlists(playlists: list[dict],  filter_func: Callable, fields: list[str]):
    """Select playlists based on filter; then reduce objects to selected keys"""
    for playlist in playlists:
        if filter(filter_func, playlist):
            yield {k: v for k, v in playlist.items() if k in fields}


def get_playlist_songs(client: Spotify, playlist_id: str, fields: str = "items(track(id, name, album(release_date), artists(id, name)))"):
    response = client.playlist_items(playlist_id, fields=fields)

    for song in response["items"]:
        song_obj = dict(
            id=song["track"]["id"],
            name=song["track"]["name"],
            release_date=song["track"]["album"]["release_date"],
            artists=song["track"]["artists"]
        )

        yield song_obj
