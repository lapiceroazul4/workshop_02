import pandas as pd
import logging
import json

from db import creating_engine, disposing_engine
from transform import drop_unnamed_column, change_categories, creating_popularity_category, ms_to_min, creating_duration_category
from transform import no_needed_columns, new_name_columns, drop_null_rows, organizing_columns, drop_duplicates, fill_na_after_merge
from drive import upload_file

# SPOTIFY ET

def read_csv():
    """
    Reads the Spotify dataset from a CSV file and returns it as a JSON string.
    
    Returns:
        str: JSON string of the Spotify dataset.
    """
    spotify_df = pd.read_csv("/home/spider/etl/workshop_02/main/spotify_dataset.csv")
    logging.info("csv read successfully")
    return spotify_df.to_json(orient='records')

def transform_csv(**kwargs):
    """
    Transforms the Spotify dataset by applying various transformations.
    
    Args:
        **kwargs: Arbitrary keyword arguments.
        
    Returns:
        str: JSON string of the transformed Spotify dataset.
    """
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_csv")
    json_data = json.loads(str_data)
    spotify_df = pd.json_normalize(data=json_data)

    spotify_df = drop_unnamed_column(spotify_df)
    spotify_df = drop_duplicates(spotify_df)
    spotify_df = change_categories(spotify_df)
    spotify_df = creating_popularity_category(spotify_df)
    spotify_df = ms_to_min(spotify_df)
    spotify_df = creating_duration_category(spotify_df)

    logging.info("csv has ended transformation process")
    return spotify_df.to_json(orient='records')
    
# GRAMMY ET

def read_db():
    """
    Reads the Grammy dataset from a database and returns it as a JSON string.
    
    Returns:
        str: JSON string of the Grammy dataset.
    """
    consulta_sql = "SELECT * FROM grammys"
    engine = creating_engine()
    grammys_df = pd.read_sql(consulta_sql, con=engine)
    disposing_engine(engine)
    logging.info("database read successfully")
    return grammys_df.to_json(orient='records')

def transform_db(**kwargs):
    """
    Transforms the Grammy dataset by applying various transformations.
    
    Args:
        **kwargs: Arbitrary keyword arguments.
        
    Returns:
        str: JSON string of the transformed Grammy dataset.
    """
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_db")
    json_data = json.loads(str_data)
    grammys_df = pd.json_normalize(data=json_data)
    logging.info("data from db has started transformation process")

    grammys_df = no_needed_columns(grammys_df)
    grammys_df = new_name_columns(grammys_df)
    grammys_df = drop_null_rows(grammys_df)

    return grammys_df.to_json(orient='records')


def merge(**kwargs):
    """
    Merges the transformed Spotify and Grammy datasets.
    
    Args:
        **kwargs: Arbitrary keyword arguments.
        
    Returns:
        str: JSON string of the merged dataset.
    """
    ti = kwargs["ti"]

    logging.info("spotify is entering to the merge function")
    str_data = ti.xcom_pull(task_ids="transform_csv")
    json_data = json.loads(str_data)
    spotify_df = pd.json_normalize(data=json_data)

    logging.info("grammys is entering to the merge function")
    str_data = ti.xcom_pull(task_ids="transform_db")
    json_data = json.loads(str_data)
    grammys_df = pd.json_normalize(data=json_data)

    df = spotify_df.merge(grammys_df, how='left', left_on='track_name', right_on='nominee')
    df = organizing_columns(df)
    df = fill_na_after_merge(df)
    logging.info("data is ready to deploy")
    return df.to_json(orient='records')
    

def load(**kwargs):
    """
    Loads the merged dataset into a database and saves it as a CSV file.
    
    Args:
        **kwargs: Arbitrary keyword arguments.
        
    Returns:
        str: JSON string of the merged dataset.
    """
    logging.info("starting load process")
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="merge")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)
    engine = creating_engine()

    df.to_sql('data-merged', engine, if_exists='replace', index=False)
    disposing_engine(engine)
    df.to_csv("data_result.csv", index=False)
    logging.info("data is ready to deploy")
    return df.to_json(orient='records')

def store(**kwargs):
    """
    Stores the final dataset by uploading it to Google Drive.
    
    Args:
        **kwargs: Arbitrary keyword arguments.
    """
    logging.info("starting store process")
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="load")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)
    
    upload_file("/home/spider/etl/workshop_02/main/data_result.csv", "1RrLEHUgBIefJGlw7jeNAPxdaHSA1hVf-")    
    logging.info("data has completed the process")