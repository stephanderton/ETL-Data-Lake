import configparser
import os

from datetime              import datetime
from pyspark.sql           import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    
    """    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    
    """   
    print("---[ process_song_data ]---")

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_song_data \
                    .filter('song_id != ""') \
                    .select(['song_id', 'title', 'artist_id', 'year', 'duration']) \
                    .dropna(how = "any", subset = ["song_id"]) \
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df_song_data \
                    .filter('artist_id != ""') \
                    .select(col('artist_id'), 
                            col('artist_name').alias('name'), 
                            col('artist_location').alias('location'),
                            col('artist_latitude').alias('latitude'),
                            col('artist_longitude').alias('longitude')) \
                    .dropna(how = "any", subset = ["artist_id"]) \
                    .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    
    """   
    print("---[ process_log_data ]---")

    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page = 'NextSong'")

    # extract columns for users table    
    users_table = df_log_data \
                    .filter('userId != ""') \
                    .select(col('userId').alias('user_id'), 
                            col('firstName').alias('first_name'), 
                            col('lastName').alias('last_name'),
                            col('gender'),
                            col('level')) \
                    .dropna(how = "any", subset = ["user_id"]) \
                    .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df_log_data = df_log_data.withColumn("timestamp", get_timestamp(df_log_data.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df_log_data = df_log_data.withColumn("datetime", get_datetime(df_log_data.ts))
    
    # extract columns to create time table
#     time_table = 
    
    # write time table to parquet files partitioned by year and month
#     time_table

    # read in song data to use for songplays table
#     song_df = 

    # extract columns from joined song and log datasets to create songplays table 
#     songplays_table = 

    # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    """
    
    """
    
    spark = create_spark_session()

#     input_data = "s3a://udacity-dend/"
# working in Udacity's workspace with smaller sample dataset
    input_data = "./data/"
    output_data = "./data/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
