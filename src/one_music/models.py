from datetime import datetime
from typing import List, Optional

from sqlmodel import SQLModel, Field, Relationship


class SongPlaylistLink(SQLModel, table=True):
    song_spotify_id: Optional[str] = Field(
        default=None, foreign_key="song.spotify_id", primary_key=True
    )
    playlist_spotify_id: Optional[str] = Field(
        default=None, foreign_key="playlist.spotify_id", primary_key=True
    )


class SongArtistLink(SQLModel, table=True):
    song_spotify_id: Optional[str] = Field(
        default=None, foreign_key="song.spotify_id", primary_key=True
    )
    artist_spotify_id: Optional[str] = Field(
        default=None, foreign_key="artist.spotify_id", primary_key=True
    )


class Playlist(SQLModel, table=True):
    spotify_id: str = Field(primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    name: str
    description: str

    songs: List["Song"] = Relationship(back_populates="playlists", link_model=SongPlaylistLink)


class Artist(SQLModel, table=True):
    spotify_id: str = Field(primary_key=True)
    name: str

    songs: List["Song"] = Relationship(back_populates="artists", link_model=SongArtistLink)


class Song(SQLModel, table=True):
    spotify_id: str = Field(primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    name: str

    playlists: List[Playlist] = Relationship(back_populates="songs", link_model=SongPlaylistLink)
    artists: List[Artist] = Relationship(back_populates="songs", link_model=SongArtistLink)
    lyrics: List["Lyrics"] = Relationship(back_populates="song")
    audio_features: "AudioFeatures" = Relationship(back_populates="song")


class Lyrics(SQLModel, table=True):
    genius_url: str = Field(primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    language: str
    file_name: str

    song_spotify_id: Optional[str] = Field(default=None, foreign_key="song.spotify_id")
    song: Optional[Song] = Relationship(back_populates="lyrics")


class AudioFeatures(SQLModel, table=True):
    """ref: https://developer.spotify.com/documentation/web-api/reference/#/operations/get-several-audio-features"""
    spotify_id: str = Field(primary_key=True, foreign_key="song.spotify_id")
    acousticness: float
    danceability: float
    duration_ms: int
    energy: float
    instrumentalness: float
    key: int  # category
    liveness: float
    mode: int  # bool 1=major, 2=minor
    speechiness: float
    tempo: float
    valence: float

    song: Optional[Song] = Relationship(back_populates="audio_features")
