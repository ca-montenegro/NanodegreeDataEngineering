import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql.types import MapType, StringType, StructType, IntegerType, DoubleType, TimestampType, StructField
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates spark session using predefined config
    Return:
    Spark: (SparkSession) Connection to the sparksession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function that read data in the input_data path folder and creates songs and artist tables
    
    Params:
    spark: (sparkSession)
    input_data:(String) file path of the input_data folder
    output_data: (String) file path of the output_data folder
    """
    # get filepath to song data file
    song_data = input_data + "*/*/*/*.json"
    
    jsonSongSchema = StructType([
        StructField("num_songs",IntegerType()),
        StructField("artist_id",StringType()),
        StructField("artist_latitud",DoubleType()),
        StructField("artist_longitud",DoubleType()),
        StructField("artist_location",StringType()),
        StructField("artist_name",StringType()),
        StructField("song_id",StringType()),
        StructField("title",StringType()),
        StructField("duration",DoubleType()),
        StructField("year",IntegerType()),  
    ])
    
    # read song data file
    df = spark.read.json(song_data,schema=jsonSongSchema)
    df.createOrReplaceTempView("songs_table_staging")

    # extract columns to create songs table
    song_table = spark.sql("""
    SELECT distinct song_id, title, artist_id, year, duration
    from songs_table_staging
    where song_id is not null
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artist_table = spark.sql("""
    select distinct artist_id, artist_name as name, artist_location as location, artist_latitud as latitude, artist_longitud as longitude
    from songs_table_staging
    where artist_id is not null
    """) 
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    Function that read data in the input_data path folder and creates users, time and songplays tables
    
    Params:
    spark: (sparkSession)
    input_data:(String) file path of the input_data folder
    output_data: (String) file path of the output_data folder
    """
    # get filepath to log data file
    log_data = input_data + "*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(dfLogs.page == 'NextSong')
    
    df.registerTempTable('logs_table_staging')

    # extract columns for users table    
    user_table = spark.sql("""
    SELECT distinct userId as user_id, firstName as first_name, lastName as last_name, gender, level
    from logs_table_staging
    where page='NextSong'
    """)

    # write users table to parquet files
    user_table.write.parquet(output_data + 'users_table/')

    
    # create datetime column from original timestamp column
    spark.udf.register("get_hour", lambda x: int(datetime.fromtimestamp(x / 1000.0).hour))
    spark.udf.register("get_day", lambda x: int(datetime.fromtimestamp(x / 1000.0).day))
    spark.udf.register("get_week", lambda x: int(datetime.fromtimestamp(x / 1000.0).isocalendar()[1]))
    spark.udf.register("get_month", lambda x: int(datetime.fromtimestamp(x / 1000.0).month))
    spark.udf.register("get_year", lambda x: int(datetime.fromtimestamp(x / 1000.0).year))
    spark.udf.register("get_dayofweek", lambda x: int(datetime.fromtimestamp(x / 1000.0).weekday()))
    
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT distinct ts as start_time, get_hour(ts) as hour, get_day(ts) as day, get_week(ts) as week, get_month(ts) as month, get_year(ts) as year, get_dayofweek(ts) as dayofweek
    from logs_table_staging
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data + 'time_table/')


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT distinct lgTable.ts as start_time, lgTable.userId, lgTable.level, sgTable.song_id, sgTable.artist_id, lgTable.sessionId as session_id, lgTable.location, lgTable.userAgent as user_agent
    from logs_table_staging as lgTable
    join songs_table_staging as sgTable
    on (lgTable.song = sgTable.title and lgTable.artist = sgTable.artist_name)
    where lgTable.page = 'NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + 'songplays_table/')


def main():
    """
    Main method of the script. Create the connection to the spark session
    
    Calls the specified functions to load song_data and log_data.
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
