import streamlit as st
import streamlit.components.v1 as components

import plotly_express as px
import umap
import numpy as np
import pandas as pd
from pandas.api.types import (
    is_categorical_dtype,
    is_numeric_dtype,
)
from sklearn.preprocessing import RobustScaler

from one_music.cohere import create_cohere_client, embed_texts
from one_music.pinecone import initialize_pinecone, get_or_create_index, query_index, fetch_vectors


@st.experimental_singleton
def boot_client():
    initialize_pinecone(st.secrets["pinecone"]["api_key"], st.secrets["pinecone"]["environment"])
    index = get_or_create_index(st.secrets["pinecone"]["index_name"], 768, metric="cosine")

    co = create_cohere_client(st.secrets["cohere"]["api_key"])

    return index, co


@st.experimental_memo
def load_lyrics_table(file_path):
    return pd.read_parquet(file_path)


@st.experimental_memo
def load_song_table(file_path, lyrics_df):
    song_df = pd.read_parquet(file_path)
    return song_df.loc[song_df.song_spotify_id.isin(lyrics_df.song_spotify_id)]


@st.experimental_memo
def load_index_table(file_path, lyrics_df):
    index_df = pd.read_parquet(file_path)
    return index_df.loc[index_df.song_spotify_id.isin(lyrics_df.song_spotify_id)]


@st.experimental_memo
def get_vectors(_pinecone_index, vector_ids):
    ids, vectors = fetch_vectors(_pinecone_index, vector_ids)
    return ids, vectors


@st.experimental_memo
def umap_reduce(X: np.ndarray) -> np.ndarray:
    return umap.UMAP().fit_transform(X)


def add_lyrics_embedding(lyrics_df, ids, vectors):
    umap_embedding = umap_reduce(np.array(vectors))
    embedding_df = pd.DataFrame(umap_embedding, index=ids, columns=["lyrics_x", "lyrics_y"]).rename_axis("vector_id").reset_index()
    return pd.merge(lyrics_df, embedding_df, on="vector_id")


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

    pinecone_index, co = boot_client()

    lyrics_df = load_lyrics_table("./data/tables/lyrics_table.parquet")
    song_df = load_song_table("./data/tables/song_table.parquet", lyrics_df)
    index_df = load_index_table("./data/tables/index_table.parquet", lyrics_df)

    # cache embeddings
    lyrics_ids, lyrics_vectors = get_vectors(pinecone_index, lyrics_df.vector_id.unique().tolist())
    lyrics_df = add_lyrics_embedding(lyrics_df, lyrics_ids, lyrics_vectors)

    song_df = add_audio_embedding(song_df, features=['acousticness', 'danceability',
                                  'duration_ms', 'energy', 'instrumentalness', 'liveness',
                                  'speechiness', 'tempo', 'valence'])

    # CONTENT
    st.title("🎶 OneMusic")
    st.markdown(
        """
        OneMusic is a music analytics tool to explore in-market and out-of-market trends. 
        """
    )

    playlist_selection = st.multiselect("Select playlist", options=index_df.playlist_name.sort_values().unique())
    if not playlist_selection:
        playlist_selection = index_df.playlist_name.sort_values().unique()

    song_df = song_df.loc[index_df.playlist_name.isin(playlist_selection)]
    lyrics_df = lyrics_df.loc[lyrics_df.song_spotify_id.isin(song_df.song_spotify_id)]

    st.header("Multilingual lyrics embedding")
    color_lyrics = st.selectbox("Color selection", options=["song_name", "language"], index=0)
    st.plotly_chart(
        px.scatter(
            data_frame=lyrics_df,
            x="lyrics_x",
            y="lyrics_y",
            hover_data=["song_name", "language", "song_spotify_id"],
            color=color_lyrics
        )
    )
    with st.expander("Multilingual lyrics embedding table"):
        if st.checkbox("Add filters", key="lyrics"):
            st.dataframe(filter_dataframe(lyrics_df))
        else:
            st.dataframe(lyrics_df)

    st.header("Audio features embedding")
    color_audio = st.selectbox("Color selection",
                                 options=['valence', 'acousticness', 'danceability',
                                          'duration_ms', 'energy', 'instrumentalness', 'liveness',
                                          'speechiness', 'tempo'],
                                 index=0
                                 )
    st.plotly_chart(
        px.scatter(
            data_frame=song_df,
            x="audio_x", y="audio_y",
            hover_data=['song_name', 'valence', 'acousticness', 'danceability',
                        'duration_ms', 'energy', 'instrumentalness', 'liveness',
                        'speechiness', 'tempo'],
            color=color_audio
        )
    )
    with st.expander("Audio features embedding table"):
        if st.checkbox("Add filters", key="audio"):
            st.dataframe(filter_dataframe(song_df))
        else:
            st.dataframe(song_df)

    with st.sidebar:
        spotify_id_request = st.text_input("Input Spotify id (from plot hover)", value="0yLdNVWF3Srea0uzk55zFn")
        components.iframe(f"https://open.spotify.com/embed/track/{spotify_id_request}?utm_source=generator")
        available_language = lyrics_df.loc[lyrics_df.song_spotify_id == spotify_id_request, "language"].sort_values()
        selected_language = st.selectbox("Select Lyrics Language", options=available_language)
        st.text(lyrics_df.loc[(lyrics_df.song_spotify_id == spotify_id_request) & (lyrics_df.language == selected_language), "lyrics_text"].values[0])


if __name__ == "__main__":
    app()
