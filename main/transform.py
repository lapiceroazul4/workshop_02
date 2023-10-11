import pandas as pd

#Transformation for Spotify

def change_categories(df):
    mapeo_generos = {
    "rock": "Rock and similars",
    "alt-rock": "Rock and similars",
    "alternative": "Rock and similars",
    "grunge": "Rock and similars",
    "punk": "Rock and similars",
    "rockabilly": "Rock and similars",
    "punk-rock": "Rock and similars",
    "hard-rock": "Rock and similars",
    "rock-n-roll": "Rock and similars",
    "psych-rock": "Rock and similars",
    "pop-film": "Pop and similars",
    "power-pop": "Pop and similars",
    "pop": "Pop and similars",
    "pop-rock": "Pop and similars",
    "synth-pop": "Pop and similars",
    "mandopop": "Pop and similars",
    "electronic": "Electronic and similars",
    "techno": "Electronic and similars",
    "detroit-techno": "Electronic and similars",
    "trance": "Electronic and similars",
    "minimal-techno": "Electronic and similars",
    "electro": "Electronic and similars",
    "deep-house": "Electronic and similars",
    "dubstep": "Electronic and similars",
    "house": "Electronic and similars",
    "progressive-house": "Electronic and similars",
    "hip-hop": "HipHop and Rap",
    "afrobeat": "HipHop and Rap",
    "rap": "HipHop and Rap",
    "trip-hop": "HipHop and Rap",   
    "reggae": "Reggae and Dancehall",
    "dancehall": "Reggae and Dancehall",
    "metal": "Metal and similars",
    "black-metal": "Metal and similars",
    "death-metal": "Metal and similars",
    "heavy-metal": "Metal and similars",
    "metalcore": "Metal and similars",
    "jazz": "Jazz and Blues",
    "blues": "Jazz and Blues",
    "classical": "Classic Music",
    "opera": "Classic Music",
    "tango": "Classic Music",
    "acoustic": "Classic Music",
    "piano": "Classic Music",
    "guitar": "Classic Music",
    "latino": "Latin Music",
    "salsa": "Latin Music",
    "samba": "Latin Music",
    "reggaeton": "Latin Music",
    "spanish": "Latin Music",
    "brazil": "Latin Music",
    "ambient": "Others",
    "comedy": "Others",
    "country": "Others",
    "disco": "Others",
    "funk": "Others",
    "gospel": "Others",
    "indie": "Others",
    "indie-pop": "Others",
    "new-age": "Others",
    "rockability": "Others",
    "soul": "Others",
    "songwriter": "Others",
    "drum-and-bass": "Others",
    "bluegrass": "Others",
    "breakbeat": "Others",
    "cantopop": "Others",
    "world-music": "Others",
    "chicago-house": "Others",
    "drum-and-bass": "Others",
    "anime": "Special Categories",
    "children": "Special Categories",
    "disney": "Special Categories",
    "kids": "Special Categories",
    "party": "Special Categories",
    "sad": "Special Categories",
    "sleep": "Special Categories",
    "study": "Special Categories",
    "study": "Special Categories",
    "study": "Special Categories",
    "study": "Special Categories",
    "singer-songwriter": "Special Categories",
    "ska": "Special Categories",
    "show-tunes": "Special Categories",
    "sertanejo": "Special Categories",
    "r-n-b": "Special Categories",
    "pagode": "Special Categories",
    "malay": "Special Categories",
    "mpb": "Special Categories",
    "industrial": "Special Categories",
    "idm": "Special Categories", 
    "honky-tonk": "Special Categories",
    "hardstyle": "Special Categories",
    "hardcore": "Special Categories",
    "happy": "Special Categories",
    "groove": "Special Categories",
    "goth": "Special Categories",
    "grindcore": "Special Categories",
    "garage": "Special Categories",
    "garage": "Special Categories",
    "garage": "Special Categories",
    "garage": "Special Categories",
    "emo": "Special Categories",
    "dub": "Special Categories",
    "edm": "Special Categories",
    "forro": "Special Categories",
    "romance": "Special Categories",
    "club": "Special Categories",
    "dance": "Special Categories",
    "folk": "Special Categories",
    "chill": "Special Categories",
    "swedish": "European Music",
    "french": "European Music",
    "turkish": "European Music",
    "iranian": "European Music",
    "german": "European Music",
    "british": "European Music",
    "j-dance":"Oriental Music",
    "j-idol":"Oriental Music",
    "j-pop":"Oriental Music",
    "j-rock":"Oriental Music",
    "k-pop":"Oriental Music",
    "indian":"Oriental Music",
    }
    df['track_genre'] = df['track_genre'].replace(mapeo_generos)
    return df

def drop_unnamed_column(df): 

    df.drop(columns=["Unnamed: 0"], axis=1, inplace=True)
    return df

def drop_duplicates(df):

    df = df.drop_duplicates(keep='first')
    return df

def creating_popularity_category(df):

    # Crear una lista de límites para las categorías
    limits = [0, 30, 60, 100]

    # Crear una lista de etiquetas para las categorías
    labels = ['Poca Popularidad', 'Media Popularidad', 'Mucha Popularidad']

    # Utilizar pd.cut() para crear la nueva columna de categorías
    df['popularity_category'] = pd.cut(df['popularity'], bins=limits, labels=labels, right=False)
    return df

def ms_to_min(df):
    df['duration_min'] = (df['duration_ms'] / 60000).round(2)  # Redondear a 2 decimales
    return df

def creating_duration_category(df):
    # Crear una lista de límites para las categorías
    limits = [0, 3, 4, float('inf')]

    # Crear una lista de etiquetas para las categorías
    labels = ['Poca Duracion', 'Media Duracion', 'Mucha Duracion']

    # Utilizar pd.cut() para crear la nueva columna de categorías
    df['duration_category'] = pd.cut(df['duration_min'], bins=limits, labels=labels, right=False)
    return df

#Transformation for Grammys

def no_needed_columns(df):
    #Elimina columnas innecesarias
    df.drop(['year', 'img', 'title', 'published_at', 
                     'updated_at', 'workers', 'artist'],axis=1,inplace=True)
    return df
    
def new_name_columns(df):
    #Cambia el nombre de las siguientes columnas
    new_columns = {
    'winner':'is_nominee',
    'category': 'grammy'
    }
    df.rename(columns=new_columns, inplace=True)
    return df

def drop_null_rows(df):
    df.drop([2261,2359,2454,2547,4525,4573], axis=0, inplace=True)
    return df

def organizing_columns(df):
    columns = ['track_id', 'artists', 'album_name', 'track_name',
       'popularity', 'popularity_category', 'duration_ms', 'duration_min', 
       'duration_category', 'explicit', 'danceability', 'energy',
       'key', 'loudness', 'mode', 'speechiness', 'acousticness',
       'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature',
       'track_genre', 'grammy', 'nominee', 'is_nominee']
    df = df[columns]
    return df

def fill_na_after_merge(df):
    df.fillna({'is_nominee':0}, inplace=True)
    return df
