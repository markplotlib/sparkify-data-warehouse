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
                                  event_id VARCHAR IDENTITY(0, 1),
                                  artist VARCHAR,
                                  auth VARCHAR,
                                  first_name VARCHAR,
                                  gender VARCHAR(1),
                                  item_in_session INT NOT NULL,
                                  last_name VARCHAR,
                                  length FLOAT,
                                  level VARCHAR(4),
                                  location VARCHAR,
                                  method VARCHAR(3),
                                  page VARCHAR,
                                  registration FLOAT,
                                  session_id INT NOT NULL,
                                  song VARCHAR,
                                  status INT NOT NULL,
                                  ts BIGINT NOT NULL,
                                  user_agent VARCHAR,
                                  user_id INT NOT NULL
                              )
""")

staging_songs_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_songs 
                              (
                                  song_id VARCHAR IDENTITY(0, 1),
                                  artist_id VARCHAR,
                                  artist_latitude NUMERIC,
                                  artist_location VARCHAR,
                                  artist_longitude NUMERIC,
                                  artist_name VARCHAR,
                                  duration NUMERIC,
                                  num_songs INT,
                                  title VARCHAR,
                                  year INT
                              )
""")

songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplays 
                         (
                             songplay_id VARCHAR IDENTITY(0, 1),
                             start_time BIGINT NOT NULL,
                             user_id VARCHAR NOT NULL,
                             level VARCHAR,
                             song_id VARCHAR,
                             artist_id VARCHAR,
                             session_id VARCHAR,
                             location VARCHAR,
                             user_agent VARCHAR
                         )
""")

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS users
                     (
                         user_id VARCHAR IDENTITY(0, 1), 
                         first_name VARCHAR, 
                         last_name VARCHAR,
                         gender VARCHAR, 
                         level VARCHAR NOT NULL
                     )
""")

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS songs 
                     (
                         song_id VARCHAR IDENTITY(0, 1),
                         title VARCHAR,
                         artist_id VARCHAR,
                         year INT,
                         duration NUMERIC
                     )
""")

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artists
                       (
                           artist_id VARCHAR IDENTITY(0, 1), 
                           name VARCHAR NOT NULL, 
                           location VARCHAR, 
                           latitude FLOAT, 
                           longitude FLOAT
                       )
""")

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time
                     (
                         start_time BIGINT IDENTITY(0, 1),
                         hour INT, 
                         day INT, 
                         week INT, 
                         month INT, 
                         year INT, 
                         weekday VARCHAR
                     )
""")

# STAGING TABLES
# temporary tables just before loading data into the Target table from the Source Table.
# main purpose: to increase ETL efficiency, and to ensure data integrity.

staging_events_copy = ("""
                       COPY staging_events
                       FROM '{}'
                       iam_role '{}'
                       region 'us-west-2'
                       json '{}'
""").format(
    config.get("S3", "LOG_DATA"),
    config.get("IAM_ROLE", "ARN"),
    config.get("S3", "LOG_JSONPATH")
)

staging_songs_copy = ("""
                      COPY staging_songs
                      FROM '{}'
                      iam_role '{}'
                      region 'us-west-2'
                      json '{}' 
""").format(
    config.get("S3", "SONG_DATA"),
    config.get("IAM_ROLE", "ARN"),
    config.get("S3", "LOG_JSONPATH")
)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
