# TODO
- understand how Spotify selects Top 50
- summarize Spotify's article
- filter unique song results
- request error

# Product vision and features
- a web app
- cluster songs by text content across languages
- an artist might want to create songs that provide coverage over the semantic space

# Notes
- remove section headers for generative step

# Story
- research: https://research.atspotify.com/2022/07/the-contribution-of-lyrics-and-acoustics-to-collaborative-understanding-of-mood/

# How it works
## Data retrieval
1. Poll the songs from all of the country "Top 50" playlists on Spotify (spotipy SDK)
2. For each song:
   1. retrieve some track features (audio features, genre) from Spotify (spotipy SDK)
   2. retrieve lyrics from Genius.com (lyricsgenius SDK)
   3. create an object combining lyrics + features
   4. Embed and store object (Weaviate + Cohere)
## Visualization / Exploration


# Reference
- Hypermodern Python projects: https://medium.com/@cjolowicz/hypermodern-python-d44485d9d769
- Spotipy urllib3 manual fix
- Lyricsgenius fix: https://github.com/johnwmillr/LyricsGenius/pull/215/commits/62e14d53d2978e76396556c61988986120e15022
