import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import date, datetime
import datetime
import sqlite3

# Refresh token @ https://developer.spotify.com/console/get-recently-played/?limit=&after=&before=

DATABASE_LOCATION = "sqlite:///spotify_playlist.sqlite"
USER_ID = "Stas Papanaga"
TOKEN = "BQCVXpp6EMXDauzRilNL4C1cmGaAZKrF-m6X1qtl0M5_18mxh1NI9Ypnih_jlc8Gju3_LKNPclX3R7uedGOa1kPvS7lHjlhohtsM6UUCa6sM2-5SRGGvplR1PYkX7M-iPEXrsDaQUhBwZw687W3Z_A"


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("DB is empty, terminating execution")
        return False 

    # Primary Key Check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are from the last 24 hours
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') < yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True

if __name__ == "__main__":

    # Extract data from API
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    
    # Convert time to Unix timestamp in miliseconds      
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours      
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object      
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        
    # Prepare a dictionary in order to turn it into a pandas dataframe        
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    
    # Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('spotify_playlist.sqlite')
    cursor = conn.cursor()

    sql_query_drop_table = """
    DROP TABLE IF EXISTS spotify_playlist
    """
    sql_query_create_table = """
    CREATE TABLE IF NOT EXISTS spotify_playlist(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    # Refresh table if already exists
    cursor.execute(sql_query_drop_table)
    print("Existing DB Deleted")

    cursor.execute(sql_query_create_table)
    print("DB Opened succesfully")

    try:
        song_df.to_sql("spotify_playlist", engine, index=False, if_exists='append')
    except Exception as e:
        print(f"There was an issue loading the data - {e} ")

    conn.close()
    print("DB Closed succesfully")