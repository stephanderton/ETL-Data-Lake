import configparser
import os

from datetime              import datetime
from pyspark.sql           import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.functions import to_timestamp
from pyspark.sql.types     import IntegerType
from pyspark.sql           import Row, functions as F
from pyspark.sql.window    import Window

import time
import datetime


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
    df_song_data = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_song_data \
                    .filter('song_id != ""') \
                    .select(['song_id', 'title', 'artist_id', 'year', 'duration']) \
                    .dropna(how = "any", subset = ["song_id"]) \
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
               .partitionBy("year", "artist_id") \
               .parquet(output_data + "songs")

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
#     log_data = input_data + "log-data/*/*/*.json"   # with S3 bucket
    log_data = input_data + "log-data/*.json"    # local workspace

    # read log data file
    df_log_data = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log_data = df_log_data.where("page = 'NextSong'")

    # extract columns for users table    
    users_table = df_log_data \
                    .filter('userId != ""') \
                    .select(col('userId').alias('user_id'), 
                            col('firstName').alias('first_name'), 
                            col('lastName').alias('last_name'),
                            col('gender'),
                            col('level') ) \
                    .dropna(how = "any", subset = ["user_id"]) \
                    .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(int(x / 1000)) \
                                          .strftime('%Y-%m-%d %H:%M:%S'))
    df_log_data = df_log_data.withColumn( "timestamp"
                                         , to_timestamp(get_timestamp(df_log_data.ts)))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(int(x / 1000)) \
                                         .strftime('%Y-%m-%d %H:%M:%S'))
    df_log_data = df_log_data.withColumn( "datetime"
                                         , get_datetime(df_log_data.ts))
    
    # extract columns to create time table
    time_table = df_log_data.select \
                    ( col('timestamp').alias('start_time')
                    , hour('datetime').alias('hour')
                    , dayofmonth('datetime').alias('day')
                    , weekofyear('datetime').alias('week')
                    , month('datetime').alias('month')
                    , year('datetime').alias('year')
                    , date_format('datetime', 'F').alias('weekday')
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write \
              .partitionBy("year", "month") \
              .parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df_log_data \
                        .join( song_df
                             , (df_log_data.song   == song_df.title) & \
                               (df_log_data.artist == song_df.artist_name)
                             , 'left_outer') \
                        .select( col("timestamp").alias("start_time")
                               , col("userId").alias("user_id")
                               , df_log_data.level
                               , song_df.song_id
                               , song_df.artist_id
                               , col("sessionId").alias("session_id")
                               , df_log_data.location
                               , col("useragent").alias("user_agent")
                               , year("datetime").alias("year")
                               , month("datetime").alias("month") )

    # EXTRA step: add songplay_id column to the songplays table
    songplays_table = songplays_table \
                        .select( 'start_time', 'user_id', 'level', 'song_id'
                               , 'artist_id', 'session_id', 'location', 'user_agent'
                               , 'year', 'month'
                               , F.row_number() \
                                  .over( Window.partitionBy("year", "month") \
                                               .orderBy( col("start_time").desc()
                                                       , col("user_id").desc() ) ) \
                                  .alias("songplay_id") )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month") \
                   .parquet(output_data + "songplays")


def main():
    """
    
    """
    
    spark = create_spark_session()

#     input_data = "s3a://udacity-dend/"
#     output_data = "s3a://---my-own-S3-bucket---/dend-proj4/"

    # working in Udacity's workspace with smaller sample dataset
    input_data = "./data/"
    output_data = "./data/output/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    print("---[ DONE! ]---")



if __name__ == "__main__":
    main()
