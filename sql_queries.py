import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
# fact table
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
# dimension tables (4)
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                              CREATE TABLE IF NOT EXISTS staging_events
                              (
                                    artist TEXT,
                                    auth TEXT,
                                    first_name TEXT,
                                    gender TEXT,
                                    item_in_session INT,
                                    last_name TEXT,
                                    length FLOAT,
                                    level TEXT,
                                    location TEXT,
                                    method TEXT,
                                    page TEXT,
                                    registration FLOAT,
                                    session_id BIGINT,
                                    song TEXT,
                                    status INT,
                                    ts BIGINT,
                                    user_agent TEXT,
                                    user_id INT
                              )
""")

staging_songs_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_songs
                              (
                                  num_songs INT,
                                  artist_id TEXT,
                                  artist_name TEXT,
                                  artist_latitude FLOAT,
                                  artist_longitude FLOAT,
                                  artist_location TEXT,
                                  song_id TEXT,
                                  title TEXT,
                                  duration FLOAT,
                                  year INT
                              )
""")

songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplays
                         (
                             songplay_id INT IDENTITY(0, 1) PRIMARY KEY,
                             start_time BIGINT NOT NULL,
                             user_id TEXT NOT NULL,
                             level TEXT,
                             song_id TEXT NOT NULL,
                             artist_id TEXT NOT NULL,
                             session_id TEXT,
                             location TEXT,
                             user_agent TEXT
                         )
""")

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS users
                     (
                         user_id INT IDENTITY(0, 1) PRIMARY KEY,
                         first_name TEXT,
                         last_name TEXT,
                         gender TEXT,
                         level TEXT
                     )
""")

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS songs
                     (
                         song_id INT IDENTITY(0, 1) PRIMARY KEY,
                         title TEXT,
                         artist_id TEXT NOT NULL,
                         year INT,
                         duration FLOAT
                     )
""")

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artists
                       (
                           artist_id INT IDENTITY(0, 1) PRIMARY KEY,
                           name TEXT,
                           location TEXT,
                           latitude FLOAT,
                           longitude FLOAT
                       )
""")

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time
                     (
                         start_time BIGINT IDENTITY(0, 1) PRIMARY KEY,
                         hour INT,
                         day INT,
                         week INT,
                         month INT,
                         year INT,
                         weekday TEXT
                     )
""")

# STAGING TABLES
# temporary tables just before loading data into the Target table from the Source Table.
# main purpose: to increase ETL efficiency, and to ensure data integrity.

staging_events_copy = ("""
                       COPY staging_events
                       FROM '{}'
                       iam_role '{}'
                       FORMAT AS json '{}'
                       TRUNCATECOLUMNS
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
                      COPY staging_songs
                      FROM '{}'
                      iam_role '{}'
                      FORMAT AS json 'auto'
                      TRUNCATECOLUMNS
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
                         INSERT INTO songplays
                         (start_time, user_id, level, song_id,
                          artist_id, session_id, location, user_agent)
                         SELECT DISTINCT se.ts, se.userId, se.level, 
                            ss.song_id, ss.artist_id, 
                            se.sessionId, se.location, se.userAgent
                         FROM staging_events se
                         JOIN staging_songs ss 
                            ON (se.artist = ss.artist_name)
                            AND (se.song = ss.title)
                         WHERE page = 'NextSong'
                         AND ts IS NOT NULL;
""")

user_table_insert = ("""
                     INSERT INTO users
                     (user_id, first_name, last_name, gender, level)
                     SELECT DISTINCT userId, firstName, lastName, gender, level
                     FROM staging_events
                     WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
                     INSERT INTO songs
                     (song_id, title, artist_id, year, duration)
                     SELECT DISTINCT song_id, title, artist_id, year, duration
                     FROM staging_songs
                     WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
                       INSERT INTO artists
                       (artist_id, name, location, latitude, longitude)
                       SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                       FROM staging_songs
                       WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
                     INSERT INTO time
                     (start_time, hour, day, week, month, year, weekday)
                     SELECT DISTINCT ts, 
                     EXTRACT (HOUR FROM ts) AS hour, 
                     EXTRACT (DAY FROM ts) AS day, 
                     EXTRACT (WEEK FROM ts) AS week, 
                     EXTRACT (MONTH FROM ts) AS month, 
                     EXTRACT (YEAR FROM ts) AS year, 
                     EXTRACT (WEEKDAY FROM ts) AS weekday
                     FROM staging_events
                     WHERE ts IS NOT NULL;
""")

# QUERY LISTS

# create_tables.py
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# etl.py
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
