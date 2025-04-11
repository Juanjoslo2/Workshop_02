# Importing the necessary modules
# --------------------------------

from extract.api_extract import extract_musicbrainz
from extract.spotify_extract import extracting_spotify_data
from extract.grammys_extract import extracting_grammys_data

from transform.api_transform import transform_artist_data
from transform.spotify_transform import transforming_spotify_data
from transform.grammys_transform import transforming_grammys_data
from transform.merge import merging_datasets

from load_store.load import loading_merged_data
from load_store.store import storing_merged_data

import os
import json
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


def extract_api():
    try:
        df = extract_musicbrainz()
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error extracting data: {e}")

def extract_spotify():
    try:
        df = extracting_spotify_data("./data/spotify_dataset.csv") 
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error extracting data: {e}")

def extract_grammys():
    try:
        df = extracting_grammys_data()
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        
def transform_api(df):
    try:
        json_df = json.loads(df)
        
        raw_df = pd.DataFrame(json_df)
        df = transform_artist_data(raw_df)
        
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        
def transform_spotify(df):
    try:
        json_df = json.loads(df)
        
        raw_df = pd.DataFrame(json_df)
        df = transforming_spotify_data(raw_df)
        
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        
def transform_grammys(df):
    try:
        json_df = json.loads(df)
        
        raw_df = pd.DataFrame(json_df)
        df = transforming_grammys_data(raw_df)
        
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        
def merge_data(api_data, spotify_data, grammys_data):
    try:
        api_json = json.loads(api_data)
        spotify_json = json.loads(spotify_data)
        grammys_json = json.loads(grammys_data)
        
        api_df = pd.DataFrame(api_json)
        spotify_df = pd.DataFrame(spotify_json)
        grammys_df = pd.DataFrame(grammys_json)
        
        df = merging_datasets(api_df, spotify_df, grammys_df)
        
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error merging data: {e}")
        
def load_data(df):
    try:
        json_df = json.loads(df)
        
        df = pd.DataFrame(json_df)
        loading_merged_data(df, "merged_data")
        
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        
def store_data(df):
    try:
        json_df = json.loads(df)
        
        df = pd.DataFrame(json_df)
        storing_merged_data("merged_data", df)
    except Exception as e:
        logging.error(f"Error storing data: {e}")