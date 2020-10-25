import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create= ("""
                              CREATE TABLE IF NOT EXISTS staging_events (
                              artist varchar,
                              auth varchar,
                              first_name varchar,
                              gender varchar(1),
                              item_in_session int NOT NULL,
                              last_name varchar,
                              length float,
                              level varchar(4),
                              location varchar,
                              method varchar(3),
                              page varchar,
                              registration float,
                              session_id int NOT NULL,
                              song varchar,
                              status int NOT NULL,
                              ts timestamp,
                              user_agent varchar,
                              user_id int NOT NULL
                              )
""")

staging_songs_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_songs (
                              artist_id varchar,
                              artist_latitude numeric,
                              artist_location varchar,
                              artist_longitude numeric,
                              artist_name varchar,
                              duration numeric,
                              num_songs int,
                              song_id varchar,
                              title varchar,
                              year int
                              )
""")
# staging_songs

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES
# temporary tables just before loading data into the Target table from the Source Table.
# main purpose: to increase ETL efficiency, and to ensure data integrity.

staging_events_copy = ("""
""").format()
# https://knowledge.udacity.com/questions/120438
# https://knowledge.udacity.com/questions/359452
# timestamp in staging_events create needs to be bigint.

staging_songs_copy = ("""
""").format()
# https://knowledge.udacity.com/questions/55466
# https://knowledge.udacity.com/questions/51992

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

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
