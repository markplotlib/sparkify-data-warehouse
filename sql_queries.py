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
                                    artist text,
                                    auth text,
                                    first_name text,
                                    gender text,
                                    item_in_session integer,
                                    last_name text,
                                    length float,
                                    level text,
                                    location text,
                                    method text,
                                    page text,
                                    registration float,
                                    session_id bigint,
                                    song text,
                                    status integer,
                                    ts bigint,
                                    user_agent text,
                                    user_id int
                              )
""")

staging_songs_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_songs
                              (
                                  num_songs int,
                                  artist_id text,
                                  artist_name text,
                                  artist_latitude numeric,
                                  artist_longitude numeric,
                                  artist_location text,
                                  song_id text,
                                  title text,
                                  duration numeric,
                                  year int
                              )
""")

songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplays
                         (
                             songplay_id text,
                             start_time bigint,
                             user_id text,
                             level text,
                             song_id text,
                             artist_id text,
                             session_id text,
                             location text,
                             user_agent text
                         )
""")

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS users
                     (
                         user_id text,
                         first_name text,
                         last_name text,
                         gender text,
                         level text
                     )
""")

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS songs
                     (
                         song_id text,
                         title text,
                         artist_id text,
                         year int,
                         duration numeric
                     )
""")

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artists
                       (
                           artist_id text,
                           name text,
                           location text,
                           latitude float,
                           longitude float
                       )
""")

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time
                     (
                         start_time bigint,
                         hour int,
                         day int,
                         week int,
                         month int,
                         year int,
                         weekday text
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
                         INSERT INTO songplays
                         (songplay_id, start_time, user_id, level, song_id,
                          artist_id, session_id, location, user_agent)
                         SELECT DISTINCT event_id, start_time, user_id, level, song_id,
                          artist_id, session_id, location, user_agent
                         FROM staging_events
                         ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""
                     INSERT INTO users
                     (user_id, first_name, last_name, gender, level)
                     SELECT DISTINCT user_id, first_name, last_name, gender, level
                     FROM staging_events
                     ON CONFLICT (user_id) DO NOTHING;
""")

song_table_insert = ("""
                     INSERT INTO songs
                     (song_id, title, artist_id, year, duration)
                     SELECT DISTINCT song_id, title, artist_id, year, duration
                     FROM staging_songs
                     ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
                       INSERT INTO artists
                       (artist_id, name, location, latitude, longitude)
                       SELECT DISTINCT artist_id, name, location, artist_latitude, artist_location
                       FROM staging_songs
                       ON CONFLICT (artist_id) DO NOTHING;
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
                     ON CONFLICT (start_time) DO NOTHING;
""")

# QUERY LISTS

# create_tables.py
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# etl.py
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
