"""SQL queries for ETL project"""

DROP_SONGPLAYS_TABLE = "DROP TABLE IF EXISTS songplays;"
DROP_USERS_TABLE = "DROP TABLE IF EXISTS users;"
DROP_SONGS_TABLE = "DROP TABLE IF EXISTS songs;"
DROP_ARTISTS_TABLE = "DROP TABLE IF EXISTS artists;"
DROP_TIME_TABLE = "DROP TABLE IF EXISTS time;"
DROP_STAGING_SONGS_TABLE = "DROP TABLE IF EXISTS staging_songs;"
DROP_STAGING_EVENTS_TABLE = "DROP TABLE IF EXISTS staging_events;"

SELECT_ERRORS = "SELECT * FROM sys_load_error_detail;"
DATA_QUALITY_CHECK = "SELECT COUNT(*) FROM {};"

DROP_TABLE_STATEMENTS = [
    DROP_SONGPLAYS_TABLE,
    DROP_USERS_TABLE,
    DROP_SONGS_TABLE,
    DROP_ARTISTS_TABLE,
    DROP_TIME_TABLE,
    DROP_STAGING_SONGS_TABLE,
    DROP_STAGING_EVENTS_TABLE,
]

SONGPLAY_TABLE_INSERT = """
INSERT INTO songplays (
    playid,
    start_time,
    userid,
    level,
    songid,
    artistid,
    sessionid,
    location,
    user_agent
)
SELECT
    md5(events.sessionid || events.start_time) AS playid,
    events.start_time, 
    events.userid, 
    events.level, 
    songs.song_id AS songid, 
    songs.artist_id AS artistid, 
    events.sessionid, 
    events.location, 
    events.useragent AS user_agent
FROM (
    SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
    FROM staging_events
    WHERE page = 'NextSong'
) events
LEFT JOIN staging_songs songs
ON events.song = songs.title
   AND events.artist = songs.artist_name
   AND events.length = songs.duration
"""

USER_TABLE_INSERT = """
INSERT INTO users (
    userid,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    userid,
    firstname AS first_name,
    lastname AS last_name,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong'
"""

SONG_TABLE_INSERT = """
INSERT INTO songs (
    songid,
    title,
    artistid,
    year,
    duration
)
SELECT DISTINCT
    song_id AS songid,
    title,
    artist_id AS artistid,
    year,
    duration
FROM staging_songs
"""

ARTIST_TABLE_INSERT = """
INSERT INTO artists (
    artistid,
    name,
    location,
    lattitude,
    longitude
)
SELECT DISTINCT
    artist_id AS artistid,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS lattitude,
    artist_longitude AS longitude
FROM staging_songs
"""

TIME_TABLE_INSERT = """
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT
    start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week, 
    EXTRACT(month FROM start_time)::varchar AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dow FROM start_time)::varchar AS weekday
FROM songplays
"""


CREATE_STAGING_SONGS_TABLE = """
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int4,
    artist_id varchar(512),
    artist_name varchar(512),
    artist_latitude numeric(18,0),
    artist_longitude numeric(18,0),
    artist_location varchar(512),
    song_id varchar(512),
    title varchar(512),
    duration numeric(18,0),
    "year" int4
)
DISTSTYLE EVEN;
"""

CREATE_STAGING_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar(512),
    auth varchar(512),
    firstname varchar(512),
    gender varchar(512),
    iteminsession int4,
    lastname varchar(512),
    length numeric(18,0),
    "level" varchar(512),
    location varchar(512),
    "method" varchar(512),
    page varchar(512),
    registration numeric(18,0),
    sessionid int4,
    song varchar(512),
    status int4,
    ts int8,
    useragent varchar(512),
    userid int4
)
DISTSTYLE EVEN;
"""

CREATE_SONGPLAYS_TABLE = """
CREATE TABLE IF NOT EXISTS songplays (
    playid varchar(32) NOT NULL,
    start_time timestamp NOT NULL,
    userid int4 NOT NULL,
    "level" varchar(512),
    songid varchar(512),
    artistid varchar(512),
    sessionid int4,
    location varchar(512),
    user_agent varchar(512),
    CONSTRAINT songplays_pkey PRIMARY KEY (playid)
)
DISTSTYLE EVEN;
"""

CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    userid int4 NOT NULL,
    first_name varchar(512),
    last_name varchar(512),
    gender varchar(512),
    "level" varchar(512),
    CONSTRAINT users_pkey PRIMARY KEY (userid)
)
DISTSTYLE ALL;
"""

CREATE_SONGS_TABLE = """
CREATE TABLE IF NOT EXISTS songs (
    songid varchar(512) NOT NULL,
    title varchar(512),
    artistid varchar(512),
    "year" int4,
    duration numeric(18,0),
    CONSTRAINT songs_pkey PRIMARY KEY (songid)
)
DISTSTYLE ALL;
"""

CREATE_ARTISTS_TABLE = """
CREATE TABLE IF NOT EXISTS artists (
    artistid varchar(512) NOT NULL,
    name varchar(512),
    location varchar(512),
    lattitude numeric(18,0),
    longitude numeric(18,0)
)
DISTSTYLE ALL;
"""

CREATE_TIME_TABLE = """
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL,
    "hour" int4,
    "day" int4,
    week int4,
    "month" varchar(512),
    "year" int4,
    weekday varchar(512),
    CONSTRAINT time_pkey PRIMARY KEY (start_time)
)
DISTSTYLE ALL;
"""

CREATE_TABLE_STATEMENTS = [
    CREATE_STAGING_EVENTS_TABLE,
    CREATE_STAGING_SONGS_TABLE,
    CREATE_SONGPLAYS_TABLE,
    CREATE_ARTISTS_TABLE,
    CREATE_SONGS_TABLE,
    CREATE_TIME_TABLE,
    CREATE_USERS_TABLE,
]

COPY_STAGING_SONGS = """
COPY staging_songs
FROM 's3://{bucket}/song-data/'
IAM_ROLE '{iam_role}'
FORMAT AS JSON '{json_format}'
REGION '{region}';
"""

COPY_STAGING_EVENTS = """
COPY staging_events
FROM 's3://{bucket}/log-data/'
IAM_ROLE '{iam_role}'
FORMAT AS JSON '{json_format}'
REGION '{region}';
"""
