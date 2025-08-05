
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import explode, col, to_date
from datetime import datetime

from awsglue.dynamicframe import DynamicFrame
path = "s3://spotify-etl-project-mohan/raw_data/to_processed/"
source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": [path]},
    format="json"
)

spotify_df = source_dyf.toDF()

def process_albums(df):
    album_df = df.withColumn("items", explode("items")).select(
    col("items.track.album.id").alias("album_id"),
    col("items.track.album.name").alias("album_name"),
    col("items.track.album.release_date").alias("release_date"),
    col("items.track.album.total_tracks").alias("total_tracks"),
    col("items.track.album.external_urls.spotify").alias("url")
    ).drop_duplicates(['album_id'])

    album_df = album_df.withColumn("release_date", to_date("release_date", "yyyy-MM-dd"))
    
    return album_df


def process_artists(df):
    artist_df = df.withColumn("items", explode("items"))
    artist_df = artist_df.withColumn("artists", explode("items.track.artists")).select(
    col("artists.id").alias("artist_id"),
    col("artists.name").alias("artist_name"),
    col("artists.href").alias("external_url")
    ).drop_duplicates(["artist_id"])
    
    return artist_df


def process_songs(df):
    song_df = df.withColumn("item", explode("items")).select(
    col("item.track.id").alias("song_id"),
    col("item.track.name").alias("song_name"),
    col("item.track.duration_ms").alias("duration_ms"),
    col("item.track.external_urls.spotify").alias("url"),
    col("item.track.popularity").alias("popularity"),
    col("item.added_at").alias("song_added"),
    col("item.track.album.id").alias("album_id"),
    col("item.track.album.artists")[0]["id"].alias("artist_id")
    ).drop_duplicates(["song_id"])

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    song_df = song_df.withColumn("song_added", to_date("song_added", "yyyy-MM-dd"))
    
    return song_df
album_df = process_albums(spotify_df)
artist_df = process_artists(spotify_df)
song_df = process_songs(spotify_df)
def write_to_s3(df, path_suffix, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": f"s3://spotify-etl-project-mohan/transformed_data/{path_suffix}/"},
        format=format_type
    )
write_to_s3(album_df, f"album_data/album_transformed_{datetime.now().strftime('%Y%m%d')}", "csv")
write_to_s3(artist_df, f"artist_data/artist_transformed_{datetime.now().strftime('%Y%m%d')}", "csv")
write_to_s3(song_df, f"songs_data/songs_transformed_{datetime.now().strftime('%Y%m%d')}", "csv")

def list_s3_objects(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix= prefix)
    keys = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('json')]
    return keys
    
bucket_name = "spotify-etl-project-mohan"
prefix = "raw_data/to_processed"
spotify_keys = list_s3_objects(bucket_name, prefix)

def move_and_delete_files(spotify_keys, Bucket):
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket' : Bucket,
            'Key': key
        }
        
        destination_key = 'raw_data/processed/' + key.split("/")[-1]
        
        s3_resource.meta.client.copy(copy_source, Bucket, destination_key)
        
        s3_resource.Object(Bucket, key).delete()
        
move_and_delete_files(spotify_keys, bucket_name)
        
job.commit()