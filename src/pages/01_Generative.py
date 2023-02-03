import time
from pathlib import Path
from requests.exceptions import RetryError

import streamlit as st
import streamlit.components.v1 as components

import cohere
import umap
import numpy as np
import pandas as pd
from pandas.api.types import (
    is_categorical_dtype,
    is_numeric_dtype,
)

from sklearn.preprocessing import RobustScaler
from sklearn.neighbors import NearestNeighbors


def wait_retry(wait_time, exceptions):
    def decorator(func):
        def newfunc(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions:
                time.sleep(wait_time)
            return func(*args, **kwargs)
        return newfunc
    return decorator


@st.experimental_singleton
def create_cohere_client() -> cohere.Client:
    return cohere.client.Client(st.secrets["cohere"]["api_key"])


@wait_retry(wait_time=60, exceptions=(RetryError,))
def embed_texts(cohere_client: cohere.Client, texts=[]):
    embeds = cohere_client.embed(
        texts=texts,
        model='multilingual-22-12',
        truncate="END"
    ).embeddings
    return embeds


@st.experimental_memo
def load_lyrics_table(file_path):
    return pd.read_parquet(file_path)


@st.experimental_memo
def load_song_table(file_path, lyrics_df):
    song_df = pd.read_parquet(file_path)
    song_df = song_df.dropna(axis=1, how="any")
    return song_df.loc[song_df.song_spotify_id.isin(lyrics_df.song_spotify_id)]


@st.experimental_memo
def load_index_table(file_path, lyrics_df):
    index_df = pd.read_parquet(file_path)
    return index_df.loc[index_df.song_spotify_id.isin(lyrics_df.song_spotify_id)]


@st.experimental_memo
def umap_reduce(X: np.ndarray) -> np.ndarray:
    return umap.UMAP().fit_transform(X)


def add_audio_embedding(song_df, features: list[str]):
    scaler = RobustScaler()
    X = scaler.fit_transform(song_df[features])
    umap_embedding = umap_reduce(X)
    embedding_df = pd.concat([song_df, pd.DataFrame(umap_embedding, columns=["audio_x", "audio_y"])], axis=1)
    return embedding_df


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Nested filter function; used to filter audio_features"""
    df = df.copy()

    modification_container = st.container()
    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    min_value=_min,
                    max_value=_max,
                    value=(_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].astype(str).str.contains(user_text_input)]

    return df


def app():
    # SETUP
    st.set_page_config(
        page_title="OneMusic",
        page_icon="https://e7.pngegg.com/pngimages/738/294/png-clipart-spotify-logo-podcast-music-matty-carter-ariel-pink-spotify-icon-logo-preview.png",
        layout="centered",
    )

    co = create_cohere_client()

    base_path = Path(__file__).parent.parent
    lyrics_df = load_lyrics_table(base_path.joinpath("data/tables/lyrics_table.parquet"))
    song_df = load_song_table(base_path.joinpath("data/tables/song_table.parquet"), lyrics_df)
    index_df = load_index_table(base_path.joinpath("data/tables/index_table.parquet"), lyrics_df)

    features = RobustScaler().fit_transform(song_df[['valence', 'acousticness', 'danceability', 'duration_ms',
                                                     'energy', 'instrumentalness', 'liveness', 'speechiness', 'tempo']]
                                            )

    nbrs = NearestNeighbors(n_neighbors=4).fit(features)
    distances, idx = nbrs.kneighbors(features)
    augmented_knn = pd.concat([song_df, pd.DataFrame(idx[:, 1:], columns=["nn1", "nn2", "nn3"])], axis=1)

    # CONTENT
    st.title("🎶 OneMusic")

    st.subheader("Audio features embedding table")
    if st.checkbox("Add filters", key="audio"):
        st.dataframe(filter_dataframe(song_df))
    else:
        st.dataframe(song_df)

    spotify_id_generate = st.text_input("Input Spotify id to Generate Lyrics", value="0yLdNVWF3Srea0uzk55zFn")

    snippets = ""
    for idx, row in lyrics_df.sample(3).iterrows():
        lyrics_txt = row["lyrics_text"]
        end = max(idx*100, len(lyrics_txt))
        start = end-100
        snippets += lyrics_txt[start:end]

    response = co.generate(
        model="xlarge",
        prompt="Write song lyrics based on the three following snippets: " + snippets,
        max_tokens=300,
        temperature=2,
    )
    novel_song = response.generations[0].text
    st.write(novel_song)

    with st.sidebar:
        spotify_id_request = st.text_input("Input Spotify id (from plot hover)", value="0yLdNVWF3Srea0uzk55zFn")
        components.iframe(f"https://open.spotify.com/embed/track/{spotify_id_request}?utm_source=generator")
        available_language = lyrics_df.loc[lyrics_df.song_spotify_id == spotify_id_request, "language"].sort_values()
        selected_language = st.selectbox("Select Lyrics Language", options=available_language)
        st.text(lyrics_df.loc[(lyrics_df.song_spotify_id == spotify_id_request) & (lyrics_df.language == selected_language), "lyrics_text"].values[0])


if __name__ == "__main__":
    app()
