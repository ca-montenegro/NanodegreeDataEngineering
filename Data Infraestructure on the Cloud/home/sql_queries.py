import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS songs;"
artist_table_drop = "DROP table IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist varchar,
auth varchar,
first_name varchar,
gender char,
item_in_session int,
last_name varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration float,
session_id int,
song varchar,
status int,
ts timestamp, 
user_agent varchar,
user_id int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
song_id varchar PRIMARY KEY,
num_songs int,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
title varchar,
duration float,
year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
start_time timestamp NOT NULL,
user_id int NOT NULL,
level varchar,
song_id varchar,
artist_id varchar,
session_id int NOT NULL,
location varchar,
user_agent varchar,
PRIMARY KEY (user_id, session_id, song_id));
""")
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY, 
first_name varchar NOT NULL, 
last_name varchar, 
gender char, 
level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY, 
title varchar NOT NULL, 
artist_id varchar NOT NULL, 
year int NOT NULL, 
duration float NOT NULL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY, 
name varchar NOT NULL, 
location varchar, 
latitude float, 
longitude float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY, 
hour int NOT NULL, 
day int NOT NULL, 
weekofyear int NOT NULL, 
month int NOT NULL, 
year int NOT NULL, 
weekday int NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events
from {}
iam_role {}
region 'us-west-2'
timeformat 'epochmillisecs'
json {};
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE','ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs
from {}
iam_role {}
region 'us-west-2'
json 'auto';
""").format(config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(
SELECT distinct ts, user_id, level, song_id, artist_id, session_id, location, user_agent
from staging_events
join staging_songs
on (staging_events.song = staging_songs.title and staging_events.artist = staging_songs.artist_name)
where page = 'NextSong'
)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
(
SELECT distinct user_id, first_name, last_name, gender, level
from staging_events
where page='NextSong'
)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
(
SELECT distinct song_id, title, artist_id, year, duration
from staging_songs
where song_id is not null
)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
(
select distinct artist_id, artist_name, artist_location,artist_latitude, artist_longitude
from staging_songs
where artist_id is not null
)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, weekofyear, month, year, weekday)
(
SELECT distinct start_time, extract(hour from start_time), extract (d from start_time), extract(w from start_time), extract(mon from start_time), extract(y from start_time), extract(dayofweek from start_time)
from songplays
)
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
