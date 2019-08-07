import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the song info and used to populate the songplay fact and artist dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: song data file path. 

    Returns:
        None
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.iloc[0][['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    
    # year and duration are numpy data type, like int64. Need to convert to original 
    song_data[-2] = song_data[-2].item()
    song_data[-1] = song_data[-1].item()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.iloc[0][['artist_id','artist_name', 'artist_location','artist_latitude','artist_longitude']].values.tolist()
    
    # Convert numpy datatype to python datatype
    artist_data[-2] = artist_data[-2].item()
    artist_data[-1] = artist_data[-1].item()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    import datetime
    def convert(ms):
        return datetime.datetime.fromtimestamp(ms/1000.0)

    df['ts'] = df['ts'].apply(convert)
    df['hour'] = df['ts'].dt.hour
    df['day'] = df['ts'].dt.day
    df['week_of_year'] = df['ts'].dt.week
    df['month'] = df['ts'].dt.month
    df['year'] = df['ts'].dt.year
    df['weekday'] = df['ts'].dt.dayofweek
    
    # insert time data records
    column_labels = ['ts','hour','day','week_of_year','month','year','weekday']
    time_df = df[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

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
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function can be used to read the file in the filepath 
    to get all files under a perticular file path, depending on what type of files
    function func will be invoked to process a perticular type of file

    Arguments:
        cur: the cursor object.
        conn: the connection to designated database
        filepath: data file path. 
        func: either process_log_file or process_song_file

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
    Description: This the entry point of the program. It first builds the connection
    to the designated database, the calls to process song_data and long_data files
    in order to populate fact and dim tables in that database

    Arguments:
        None

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