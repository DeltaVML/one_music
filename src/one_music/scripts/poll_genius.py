import time
import random

import hydra
from sqlmodel import Session, select

from ..models import Song, Lyrics
from ..database import engine, create_db_and_tables, get_or_create

from ..genius import (
    create_genius_client,
    search_song,
    crawl_for_translations,
    get_song_lyrics,
    generate_file_name,
    save_lyrics_to_file,
)
from ..cohere import (
    create_cohere_client,
    detect_lyrics_language,
)


@hydra.main(config_name="app.yaml", config_path="../../config", version_base="1.2")
def poll_genius(cfg) -> None:

    cohere_client = create_cohere_client(cfg.cohere.api_key)
    genius_client = create_genius_client(cfg.genius.client_token)

    with Session(engine) as session:
        query = select(Song, Lyrics).join(Lyrics, isouter=True)
        results = session.exec(query)

        for song, lyrics in results:
            if lyrics is not None:
                continue
            # NOTE Genius could return translations as primary result
            song_genius = search_song(client=genius_client, song_name=song.name, artist_name=song.artists[0].name)

            if song_genius is None:  # couldn't find a result for query
                continue
            elif song_genius.lyrics is None:  # some song page are blank
                continue
            elif song_genius.url in ['https://genius.com/Lao-ma--annotated', 'https://genius.com/Gazapizm-heyecan-yok-lyrics']:
                continue

            lyrics_snippet = song_genius.lyrics[200:]  # selecting end of text because beginning has variable headers
            detected_language_name, detected_language_code = detect_lyrics_language(cohere_client, lyrics_snippet)

            file_name = generate_file_name(song_genius.url)
            save_lyrics_to_file(lyrics=song_genius.lyrics, file_name=file_name, save_dir=cfg.genius.save_dir)

            lyrics_record = dict(
                genius_url=song_genius.url,
                song_spotify_id=song.spotify_id,
                language=detected_language_code,
                file_name=file_name,
            )
            lyrics_obj = get_or_create(session, lyrics_record, Lyrics, "song_spotify_id")
            session.add(lyrics_obj)

            for translation_url, scraped_language in crawl_for_translations(song_genius.url):
                if translation_url is None:  # exhaust crawling results
                    break

                translation_lyrics = get_song_lyrics(client=genius_client, song_url=translation_url)
                if translation_lyrics is None:  # some song page are blank
                    continue

                lyrics_snippet = translation_lyrics[:200]
                detected_language_name, detected_language_code = detect_lyrics_language(cohere_client, lyrics_snippet)

                if scraped_language in ["Romanization", "romanization"]:
                    detected_language_code += "_rom"

                file_name = generate_file_name(song_genius.url)
                save_lyrics_to_file(translation_lyrics, file_name=file_name, save_dir=cfg.genius.save_dir)

                lyrics_record = dict(
                    genius_url=translation_url,
                    song_spotify_id=song.spotify_id,
                    language=detected_language_code,
                    file_name=file_name,
                )
                lyrics_obj = get_or_create(session, lyrics_record, Lyrics, "song_spotify_id")
                session.add(lyrics_obj)

                time.sleep(30 + random.randint(15, 30))  # sleep for Genius API calls

            session.commit()


if __name__ == "__main__":
    create_db_and_tables()
    poll_genius()
