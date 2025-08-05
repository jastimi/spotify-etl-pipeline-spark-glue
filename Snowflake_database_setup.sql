CREATE DATABASE spotify_db;

create or replace storage integration s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::963619196077:role/spotify-spark-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-mohan')
    COMMENT = 'creating connection to S3';


DESCRIBE integration s3_init;

CREATE OR REPLACE file format csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;

DESCRIBE file format csv_fileformat;


CREATE OR REPLACE stage spotify_stage
    URL = 's3://spotify-etl-project-mohan/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat;


LIST @spotify_stage/songs;

CREATE OR REPLACE TABLE tbl_album (
    album_id STRING,
    name STRING,
    release_date DATE,
    total_tracks INT,
    url STRING
);

CREATE OR REPLACE TABLE tbl_artist (
    artist_id STRING,
    name STRING,
    url STRING
);

CREATE OR REPLACE TABLE tbl_songs (
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING
);


COPY INTO tbl_album
FROM @spotify_stage/album_data/album_transformed_20250803/run-1754197534797-part-r-00000;

COPY INTO tbl_artist
FROM @spotify_stage/artist_data/artist_transformed_20250803/run-1754197672031-part-r-00000;

COPY INTO tbl_songs
FROM @spotify_stage/songs_data/songs_transformed_20250803/run-1754197929736-part-r-00000;



CREATE OR REPLACE SCHEMA pipe;

CREATE OR REPLACE pipe pipe.tbl_songs_pipe 
auto_ingest = TRUE 
AS 
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_stage/songs_data/;

CREATE OR REPLACE pipe pipe.tbl_artist_pipe 
auto_ingest = TRUE 
AS 
COPY INTO spotify_db.public.tbl_artist
FROM @spotify_db.public.spotify_stage/artist_data/;

CREATE OR REPLACE pipe pipe.tbl_album_pipe 
auto_ingest = TRUE 
AS 
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/album_data/;

DESC pipe pipe.tbl_album_pipe;

SELECT SYSTEM$PIPE_STATUS('pipe.tbl_album_pipe');

select count(*) from tbl_songs; 

SELECT * from tbl_songs order by popularity desc;
