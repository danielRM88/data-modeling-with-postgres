import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads a song metadata file from path

    - Extracts songs and artists required fields

    - Stores the data in their respective tables
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = (list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]))
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]))
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads user activity log file from path

    - Parses and transforms start date timestamp values

    - Saves pandas data frame with time data to csv

    - Executes postgres COPY command to read file and insert all records in bulk

    - Extracts user information into pandas data frame

    - Saves pandas data frame with user data to csv

    - Executes postgres COPY command to read file and insert all records in bulk

    - Reads songpplay information and for each record finds the associated song and artist

    - This is saved in a pandas data frame which is saved as csv

    - Executes postgres COPY command to read file and insert all records in bulk
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t.values, t.dt.hour.values, t.dt.day.values, t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(list(zip(*time_data)), columns=column_labels)

    # insert in bulk using the postgres COPY command
    time_df.to_csv('./time_tmp.csv', index=False, header=False)
    cur.execute(time_table_copy_insert, ('/home/workspace/time_tmp.csv',))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert in bulk using the postgres COPY command
    user_df.to_csv('./users_tmp.csv', index=False, header=False)
    cur.execute(users_table_copy_insert, ('/home/workspace/users_tmp.csv',))

    songplay_df = []
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_df.append([pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent])

    songplay_df = pd.DataFrame(songplay_df)
    songplay_df.to_csv('./songplay_tmp.csv', index=False, header=False)
    cur.execute(songplay_table_copy_insert, ('/home/workspace/songplay_tmp.csv',))

def process_data(cur, conn, filepath, func):
    """
    Process all files in a given directory with the specified function
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
    - Grabs database connection

    - Process all files in the data/song_data directory with the process_song_files function

    - Process all files in the data/log_data directory with the process_log_files function
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()