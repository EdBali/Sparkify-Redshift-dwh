import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


#configuration settings
LOG_DATA = config.get('S3','LOG_DATA')
JSON_LOG_PATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE','ARN')
# ACCESS_KEY = config.get('AWS','KEY')
# SECRET = config.get('AWS','SECRET')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES
fields = ['artist','auth','firstName','gender','itemInSession','lastName','length','level','location','method','page','registration','sessionId','song','status','ts','userAgent','userId']
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events_table(
        artist_name varchar,
        auth varchar,
        firstName varchar,
        gender varchar(1),
        itemInSession int ,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration float,
        sessionId int,
        song varchar,
        status int,
        ts bigint ,
        userAgent varchar,
        userId int
    )
    diststyle auto;
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs_table(
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year INT)
    diststyle auto;
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table(
        songplay_id bigint IDENTITY(0,1) PRIMARY KEY, 
        start_time bigint NOT NULL UNIQUE, 
        user_id int NOT NULL UNIQUE,
        level varchar NOT NULL, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location varchar,
        user_agent varchar

    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table(
        user_id int PRIMARY KEY,
        songplay_id varchar,
        first_name varchar NOT NULL,
        last_name varchar NOT NULL,
        gender varchar NOT NULL,
        level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_table(
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL,
        artist_id varchar NOT NULL,
        year varchar,
        duration numeric
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_table(
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        latitude varchar,
        longitude varchar
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table(
        start_time varchar PRIMARY KEY,
        hour varchar,
        day varchar NOT NULL,
        week int,
        month varchar NOT NULL,
        year varchar NOT NULL,
        weekday varchar
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table
    FROM {}
    credentials 'aws_iam_role={}'
    TIMEFORMAT 'epochmillisecs'
    region 'us-west-2'
    COMPUPDATE OFF
    json {};
""").format(LOG_DATA,IAM_ROLE,JSON_LOG_PATH)

staging_songs_copy = ("""
    COPY staging_songs_table
    FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    COMPUPDATE OFF
    json 'auto';
""").format(SONG_DATA,IAM_ROLE,)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(user_id) DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO user_table(
    user_id,first_name,last_name,gender,level)
    VALUES(%s,%s,%s,%s,%s)
    ON CONFLICT (user_id) DO UPDATE
    SET level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO song_table(
    song_id,title,artist_id,year,duration) 
    values(%s,%s,%s,%s,%s)
    ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artist_table(
    artist_id,name,location,latitude,longitude) 
    VALUES(%s,%s,%s,%s,%s)
    ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time_table(
    start_time,hour,day,week,month,year,weekday)
    VALUES(%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (start_time) DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
