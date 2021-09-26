import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
                               CREATE TABLE IF NOT EXISTS staging_events (
                                   event_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY SORTKEY,
                                   artist VARCHAR,
                                   auth VARCHAR,
                                   firstname VARCHAR,
                                   gender VARCHAR,
                                   itemInSession INTEGER,
                                   lastname VARCHAR,
                                   length FLOAT,
                                   level VARCHAR,
                                   location VARCHAR,
                                   method VARCHAR,
                                   page VARCHAR,
                                   registration BIGINT,
                                   sessionId INTEGER,
                                   song VARCHAR,
                                   status INTEGER,
                                   ts VARCHAR,
                                   userAgent VARCHAR,
                                   userId INTEGER);
                               """)

staging_songs_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_songs (
                                  num_songs INTEGER,
                                  artist_id VARCHAR NOT NULL,
                                  latitude DECIMAL,
                                  longitude DECIMAL,
                                  location VARCHAR,
                                  artist_name VARCHAR,
                                  song_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
                                  title VARCHAR,
                                  duration DECIMAL,
                                  year INTEGER);
                              """)

songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplays (
                             songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
                             start_time TIMESTAMP,
                             user_id VARCHAR,
                             level VARCHAR,
                             song_id VARCHAR NOT NULL,
                             artist_id VARCHAR NOT NULL,
                             session_id INTEGER,
                             location VARCHAR,
                             user_agent VARCHAR);
                         """)

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS users (
                         user_id INTEGER NOT NULL PRIMARY KEY,
                         first_name VARCHAR,
                         last_name VARCHAR,
                         gender VARCHAR,
                         level VARCHAR);
                     """)

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS songs (
                         song_id VARCHAR NOT NULL PRIMARY KEY,
                         title VARCHAR,
                         artist_id VARCHAR NOT NULL,
                         year INTEGER,
                         duration DECIMAL);
                     """)

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artists (
                           artist_id VARCHAR NOT NULL PRIMARY KEY,
                           name VARCHAR,
                           location VARCHAR,
                           latitude DECIMAL,
                           longitude DECIMAL);
                       """)

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time (
                         start_time TIMESTAMP NOT NULL PRIMARY KEY,
                         hour INTEGER,
                         day INTEGER,
                         week INTEGER,
                         month INTEGER,
                         year INTEGER,
                         weekday INTEGER);
                     """)

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {};
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto';
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
                    INSERT INTO songplays (start_time, 
                                            user_id, 
                                            level, 
                                            song_id, 
                                            artist_id, 
                                            session_id, 
                                            location, 
                                            user_agent) 
                    SELECT  
                        TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
                        e.userId, 
                        e.level, 
                        s.song_id,
                        s.artist_id, 
                        e.sessionId,
                        e.location, 
                        e.userAgent
                    FROM staging_events e, staging_songs s
                    WHERE e.page = 'NextSong' 
                    AND e.song = s.title 
                    AND e.artist = s.artist_name 
                    AND e.length = s.duration;
""")

user_table_insert = ("""
                     INSERT INTO users (user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
                     SELECT DISTINCT userId AS user_id,
                                     firstName AS first_name,
                                     lastName AS last_name,
                                     gender AS gender,
                                     level AS level
                     FROM staging_events
                     WHERE user_Id IS NOT NULL;
                     """)

song_table_insert = ("""
                     INSERT INTO songs (song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
                     SELECT DISTINCT song_id AS song_id,
                                     title AS title,
                                     artist_id AS artist_id,
                                     year AS year,
                                     duration AS duration
                     FROM staging_songs
                     WHERE song_id IS NOT NULL;
                     """)

artist_table_insert = ("""
                       INSERT INTO artists (artist_id,
                                            name,
                                            location,
                                            latitude,
                                            longitude)
                       SELECT DISTINCT artist_id AS artist_id,
                                       artist_name AS name,
                                       location AS location,
                                       latitude AS latitude,
                                       longitude AS longitude
                       FROM staging_songs
                       WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
                     INSERT INTO time (start_time,
                                       hour,
                                       day,
                                       week,
                                       month,
                                       year,
                                       weekday)
                     SELECT DISTINCT ts,
                     EXTRACT(hour FROM ts),
                     EXTRACT(day FROM ts),
                     EXTRACT(week FROM ts),
                     EXTRACT(month FROM ts),
                     EXTRACT(year FROM ts),
                     EXTRACT(weekday FROM ts)
                     FROM staging_events
                     WHERE ts IS NOT NULL;
                     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
