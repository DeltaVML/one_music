import pytest

from src.one_music import create_authenticator, create_client, get_user_playlists


def test_auth_manager_invalid() -> None:
    import spotipy.oauth.SpotifyOauthError

    with pytest.raises(spotipy.oauth2.SpotifyOauthError):
        auth_manager = create_authenticator(client_id="a", client_secret="b")
        auth_manager.get_access_token()


