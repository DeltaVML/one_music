from datetime import datetime
from typing import List, Optional

from sqlmodel import SQLModel, Field, Relationship


class SongPlaylistLink(SQLModel, table=True):
    song_id: Optional[str] = Field(
        default=None, foreign_key="song.id", primary_key=True
    )
    playlist_id: Optional[str] = Field(
        default=None, foreign_key="playlist.id", primary_key=True
    )

    song: "Song" = Relationship(back_populates="playlist_links")
    playlist: "Playlist" = Relationship(back_populates="song_links")


class SongArtistLink(SQLModel, table=True):
    song_id: Optional[str] = Field(
        default=None, foreign_key="song.id", primary_key=True
    )
    artist_id: Optional[str] = Field(
        default=None, foreign_key="artist.id", primary_key=True
    )

    song: "Song" = Relationship(back_populates="artist_links")
    artist: "Artist" = Relationship(back_populates="song_links")


class Playlist(SQLModel, table=True):
    id: int = Field(primary_key=True)
    spotify_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    name: str
    description: str

    song_links: List[SongPlaylistLink] = Relationship(back_populates="playlist")


class Artist(SQLModel, table=True):
    id: int = Field(primary_key=True)
    spotify_id: str
    name: str

    song_links: List[SongArtistLink] = Relationship(back_populates="artist")


class Song(SQLModel, table=True):
    id: int = Field(primary_key=True)
    spotify_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    name: str
    # release_date: datetime

    playlist_links: List[SongPlaylistLink] = Relationship(back_populates="song")
    artist_links: List[SongArtistLink] = Relationship(back_populates="song")
    lyrics: List["Lyrics"] = Relationship(back_populates="song")


class Lyrics(SQLModel, table=True):
    id: int = Field(primary_key=True)
    genius_url: str
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    language: str
    is_downloaded: bool

    song_id: Optional[str] = Field(default=None, foreign_key="song.id")
    song: Optional[Song] = Relationship(back_populates="lyrics")





