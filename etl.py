import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function is used to get the data for the songs and artists table from the provided filepath
                 after which we call the already created insert statement to do the insert to the tables

    Arguments:
        cur: the cursor object. 
        filepath: song data file path. 

    Returns:
        None
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = (df[['song_id','title','artist_id','year','duration']].values.tolist())[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist())[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    Description: This function is used to process the log data data. The data is first loaded to a dataframe,
                 some required processign is done to make it fit for insert into the time table,
                 insert into the user table is quite straight forward,
                 artists and songs table is checked throught the song_select script to right data for songplays table

    Arguments:
        cur: the cursor object. 
        filepath: song data file path. 

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    df.ts = pd.to_datetime(df.ts, unit='ms')
    t = df.ts
    
    # insert time data records
    time_data = [t.values,t.dt.hour.values,t.dt.day.values,t.dt.weekofyear.values,t.dt.month.values, \
                 t.dt.year.values,t.dt.dayofweek.values]
    column_labels = ['timestamp','hour','day','weekofyear','month','year','weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))  

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

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
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is used to get all the json files in a given filepath and pass to either the 
                 process_song_file or process_log_file as appropriate. These functions deals with only one 
                 file at a time, so this function is used to iterate through all files and process them

    Arguments:
        cur: the cursor object. 
        conn: connection to the Sparkify database
        filepath: song or log data file path. 
        func: either  process_song_file or process_log_file function
        
    Returns:
        None
    """
    
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
    
    """
    Description: The main function creates the connection to sparkify database,
                 and calls the process_data function to process the files.

    Returns:
        None
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()