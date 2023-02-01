import re
import requests

from bs4 import BeautifulSoup
import cohere
from lyricsgenius import Genius
from lyricsgenius.types import Song


def create_client(client_token: str) -> Genius:
    return Genius(client_token)


def search_song(client: Genius, song_name: str, artist_name: str = None):
    return client.search_song(song_name)  # , artist_name)


def get_song_lyrics(client: Genius, song_url: str):
    return client.lyrics(song_url=song_url, remove_section_headers=False)  # TODO parse headers before embedding


def detect_lyrics_language(cohere_client: cohere.Client, lyrics_snippet) -> str:
    response = cohere_client.detect_language(texts=[lyrics_snippet])
    language = response.results[0].language_name

    return language


def crawl_for_translations(genius_url):
    r = requests.get(genius_url, timeout=5)

    if r.status_code != requests.codes.ok:
        raise TimeoutError

    soup = BeautifulSoup(r.text, features="html.parser")

    # check if `lyrics control` section is present
    if lyrics_controls := soup.find("div", class_=re.compile("LyricsControls__Container")):
        # check if `lyrics control`contains a translation section
        if lyrics_controls.find(text="Translations"):
            # get all translations listed in `lyrics control > translations`
            translation_items = lyrics_controls.find_all("li", class_=re.compile("LyricsControls__DropdownItem"))

            for item in translation_items:
                yield item.a.get("href"), item.a.div.text  # (link, language)

    yield None, None


def save_lyrics_to_file(lyrics: str, file_name: str, save_dir: str) -> None:
    with open(f"{save_dir}/{file_name}", encoding="utf-8", mode="w") as f:
        f.write(lyrics)
