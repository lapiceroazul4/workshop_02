import pandas as pd
import logging
import json
from  Database import creating_engine, disposing_engine
from transform import drop_unnamed_column, change_categories, creating_popularity_category, ms_to_min, creating_duration_category
from transform import no_needed_columns, new_name_columns, drop_null_rows, organizing_columns
from drive import subir_archivo

#SPOTIFY ET

def read_csv():
    #Reading csv file
    spotify_df = pd.read_csv("/home/spider/etl/workshop_02/main/spotify_dataset.csv")
    logging.info("csv read succesfully")
    return spotify_df.to_json(orient='records')

def transform_csv(**kwargs):
    #CSV's transformations
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_csv")
    json_data = json.loads(str_data)
    spotify_df = pd.json_normalize(data=json_data)

    #Drop Column unnamed
    spotify_df = drop_unnamed_column(spotify_df)
    #Reducing Categories
    spotify_df = change_categories(spotify_df)
    #Creating Popularity Categorias
    spotify_df = creating_popularity_category(spotify_df)
    #Convert duration from ms to min
    spotify_df = ms_to_min(spotify_df)
    #Creating duration category
    spotify_df = creating_duration_category(spotify_df)

    logging.info("csv has ended transformation proccess")
    return spotify_df.to_json(orient='records')
    
#GRAMMY ET

def read_db():
    # Hacer la consulta SQL a Grammys
    consulta_sql = "SELECT * FROM grammys"
    # Crear el engine para conectarse a la base de datos
    engine = creating_engine()
    grammys_df = pd.read_sql(consulta_sql, con=engine)
    #Cerramos la conexion a la db
    disposing_engine(engine)

    logging.info("database read succesfully")
    return grammys_df.to_json(orient='records')

def transform_db(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_db")
    json_data = json.loads(str_data)
    grammys_df = pd.json_normalize(data=json_data)
    logging.info(f"data from db has started transformation proccess")

    #Elimina columnas innecesarias
    grammys_df = no_needed_columns(grammys_df)
    #Cambia el nombre de las siguientes columnas
    grammys_df = new_name_columns(grammys_df)
    #Eliminar registros que no aportan valor
    grammys_df = drop_null_rows(grammys_df)

    #function transformed
    return grammys_df.to_json(orient='records')


def merge(**kwargs):
    ti = kwargs["ti"]


    logging.info( f"spotify is entering to the merge function")
    str_data = ti.xcom_pull(task_ids="transform_csv")
    json_data = json.loads(str_data)
    spotify_df = pd.json_normalize(data=json_data)

    logging.info( f"grammys is entering to the merge function")
    str_data = ti.xcom_pull(task_ids="transform_db")
    json_data = json.loads(str_data)
    grammys_df = pd.json_normalize(data=json_data)

    df = spotify_df.merge(grammys_df, how='left', left_on='track_name', right_on='nominee')
    df2 = organizing_columns(df)
    logging.info( f"data is ready to deploy")
    return df2.to_json(orient='records')
    

def load(**kwargs):
    logging.info("starting load proc")
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="merge")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)
    engine = creating_engine()

    df.to_sql('data-merged', engine, if_exists='replace', index=False)

    #Cerramos la conexion a la db
    disposing_engine(engine)
    df.to_csv("data_result.csv", index=False)
    logging.info( f"data is ready to deploy")
    return df.to_json(orient='records')

def store(**kwargs):
    logging.info("starting store process")
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="load")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)
    
    subir_archivo("/home/spider/etl/workshop_02/main/data_result.csv","1RrLEHUgBIefJGlw7jeNAPxdaHSA1hVf-")    
    logging.info( f"data has completed the process")
