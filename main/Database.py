import json 
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, Date, Text, ForeignKey, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#Leer config desde el JSON
with open('db_config.json', 'r') as json_file:
    data = json.load(json_file)
    usuario = data["user"]
    password = data["passwd"]
    server = data["server"]
    database = data["database"]

db_url = f"mysql+pymysql://{usuario}:{password}@{server}/{database}?charset=utf8"
Base = declarative_base()

#Function to Create Engine
def creating_engine():
    engine = create_engine(db_url)
    return engine

#Function to create the sessions
def creating_session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

#Function to close the session
def closing_session(session):
    session.close()

#Function to Dispose Engine
def disposing_engine(engine):
    engine.dispose()
    print("engine cerrado")