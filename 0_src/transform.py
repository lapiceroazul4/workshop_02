import pandas as pd

# Transformation for Spotify

def change_categories(df):
    """
    Changes the categories of the 'track_genre' column to broader categories.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame with updated 'track_genre' categories.
    """
    genre_mapping = {
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
        "classical": "Classical Music",
        "opera": "Classical Music",
        "tango": "Classical Music",
        "acoustic": "Classical Music",
        "piano": "Classical Music",
        "guitar": "Classical Music",
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
        "j-dance": "Oriental Music",
        "j-idol": "Oriental Music",
        "j-pop": "Oriental Music",
        "j-rock": "Oriental Music",
        "k-pop": "Oriental Music",
        "indian": "Oriental Music",
    }
    df['track_genre'] = df['track_genre'].replace(genre_mapping)
    return df

def drop_unnamed_column(df):
    """
    Drops the 'Unnamed: 0' column from the DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame without the 'Unnamed: 0' column.
    """
    df.drop(columns=["Unnamed: 0"], axis=1, inplace=True)
    return df

def drop_duplicates(df):
    """
    Drops duplicate rows from the DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame without duplicate rows.
    """
    df.drop_duplicates(keep='first', inplace=True)
    return df

def create_popularity_category(df):
    """
    Creates a new column 'popularity_category' based on the 'popularity' column.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame with the new 'popularity_category' column.
    """
    limits = [0, 30, 60, 100]
    labels = ['Low Popularity', 'Medium Popularity', 'High Popularity']
    df['popularity_category'] = pd.cut(df['popularity'], bins=limits, labels=labels, right=False)
    return df

def ms_to_min(df):
    """
    Converts the 'duration_ms' column from milliseconds to minutes.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame with the new 'duration_min' column.
    """
    df['duration_min'] = (df['duration_ms'] / 60000).round(2)
    return df

def create_duration_category(df):
    """
    Creates a new column 'duration_category' based on the 'duration_min' column.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame with the new 'duration_category' column.
    """
    limits = [0, 3, 4, float('inf')]
    labels = ['Short Duration', 'Medium Duration', 'Long Duration']
    df['duration_category'] = pd.cut(df['duration_min'], bins=limits, labels=labels, right=False)
    return df

# Transformation for Grammys

def drop_unnecessary_columns(df):
    """
    Drops unnecessary columns from the Grammy DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame without unnecessary columns.
    """
    df.drop(['year', 'img', 'title', 'published_at', 'updated_at', 'workers', 'artist'], axis=1, inplace=True)
    return df

def rename_columns(df):
    """
    Renames specific columns in the Grammy DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """
    new_columns = {
        'winner': 'is_nominee',
        'category': 'grammy'
    }
    df.rename(columns=new_columns, inplace=True)
    return df

def drop_null_rows(df):
    """
    Drops rows with null values from the Grammy DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame without null rows.
    """
    df.drop([2261, 2359, 2454, 2547, 4525, 4573], axis=0, inplace=True)
    return df

def organize_columns(df):
    """
    Organizes the columns in a specific order in the merged DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame with organized columns.
    """
    columns = [
        'track_id', 'artists', 'album_name', 'track_name', 'popularity', 'popularity_category', 
        'duration_ms', 'duration_min', 'duration_category', 'explicit', 'danceability', 'energy',
        'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 
        'valence', 'tempo', 'time_signature', 'track_genre', 'grammy', 'nominee', 'is_nominee'
    ]
    df = df[columns]
    return df

def fill_na_after_merge(df):
    """
    Fills NA values in the 'is_nominee' column with 0 after merging.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        pd.DataFrame: The DataFrame with filled NA values.
    """
    df.fillna({'is_nominee': 0}, inplace=True)
    return df