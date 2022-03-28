import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import create_tables


def process_song_file(cur, filepath):
    '''
    Reads and processes song data in json format.
    
    Puts obtained information into a dataframe to organize and tranform the song data before it's inserted into the 
    database. Gather sufficient data for the songs table and the artists table from this dataframe.
    
    Parameters:
        cur (object): Cursor object that allows the Python code to execute the PostgreSQL insert commands
        filepath (str): String that identifies the folder path for the song data
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].copy().values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].copy()
    artist_data = artist_data.rename({'artist_name': 'name', 'artist_location': 'location', 'artist_latitude': 'latitude', 'artist_longitude': 'longitude'}, axis=1).values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Reads and processes log data in json format.
    
    Puts obtained information into a dataframe to organize and tranform the log data before it's inserted into the database.
    Gather sufficient data for the time table and users table from this dataframe which will then be used to populate the
    time table and execute the song_select query.
    
    Parameters:
        cur (object): Cursor object that allows the Python code to execute the PostgreSQL insert commands
        filepath (str): String that identifies the folder path for the song data
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = ([time, time.hour, time.day, time.week, time.month, time.year, time.dayofweek] for time in t)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
    user_df = user_df.rename({'userId': 'user_id', 'firstName': 'first_name', 'lastName': 'last_name'}, axis=1)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Gets all song and log data from directory.
    
    Prints information regarding number of files and iterates over every file to give processing status.
    
    Parameters:
        cur (object): Cursor object that allows the Python code to execute the PostgreSQL insert commands
        conn (connection instance): Calls the connect() function to create a new Postgres database session
        filepath (str): String that identifies the folder path for the data
        func (function): The function being called to process either song data or log data
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    ''' ETL pipeline for the sparkifydb database.'''
    create_tables.main()
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()