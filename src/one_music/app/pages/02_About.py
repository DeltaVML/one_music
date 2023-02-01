import streamlit as st
import streamlit.components.v1 as components

st.title("About")

st.markdown(
    """
    This project was realized by Thierry Jean in the context of the Cohere hackaton hosted by Lablab.ai from January 27 to February 3rd 2023. Learn more about the event and other projects [here](https://lablab.ai/event/cohere-hackathon).
    ### Architecture
    ##### Registry
    Find below the data model for the registry
    ![Data model](../static/data_model_diagram.png)
    ### Challenges
    ### Next-steps
    ### Stack
    Key technologies used for this project:
    - [Cohere API](https://docs.cohere.ai/reference/about) via [cohere](https://github.com/cohere-ai/cohere-python)
    - [Spotify API](https://developer.spotify.com/documentation/web-api/reference/#/) via [spotipy](https://spotipy.readthedocs.io/en/2.22.1/#)
    - [Genius API](https://docs.genius.com/) via [lyricsgenius](https://lyricsgenius.readthedocs.io/en/master/)
    - [requests](https://requests.readthedocs.io/en/latest/) + [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) (webscraping)
    - [Weaviate](https://weaviate.io/developers/weaviate) with [Cohere integration](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-cohere) (vector search engine)
    - [SQLModel](https://sqlmodel.tiangolo.com/) (ORM)
    - Docker and docker-compose (deployment)
    - AWS (cloud infra)    
    """
)