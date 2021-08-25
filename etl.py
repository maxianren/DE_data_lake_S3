import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_date


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"
    
    # read song data file
    df_song = spark.read.json(song_data) 

    # extract columns to create songs table
    df_song.createOrReplaceTempView("song_data")
    songs_table = spark.sql("""
    select 
        distinct (song_id) AS song_id, 
        title AS title,
        artist_id AS artist_id,
        year AS year,
        duration AS duration
    from song_data
    where song_id IS NOT NULL
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs', mode="overwrite")
    
    print("successfully created songs table in s3")

    # extract columns to create artists table
    artists_table = spark.sql("""
    select 
        distinct (artist_id) AS artist_id, 
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS lattitude,
        artist_longitude AS longitude
    from song_data
    where artist_id IS NOT NULL
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artist', mode="overwrite")
    
    print("successfully created artists table in s3")



def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table    
    df_log.createOrReplaceTempView("log_data")
    
    users_table=spark.sql("""
    select 
        distinct (userId) AS user_id, 
        firstName AS first_name,
        lastName AS last_name,
        gender AS gender,
        level AS level
    from log_data
    where userId IS NOT NULL
    """)
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users', mode="overwrite")
    
    print("successfully created users table in s3")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x/1000))
    df_log =df_log.withColumn('timestamp', get_timestamp(df_log.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    df_log = df_log.withColumn('datetime', get_datetime(df_log.timestamp))
    
    # extract columns to create time table
    time_table =  df_log.select(col('ts').alias('ts'),
                            col('timestamp').alias('start_time'),
                           hour('datetime').alias('hour'),
                           dayofmonth('datetime').alias('day'),
                           weekofyear('datetime').alias('week'),
                           month('datetime').alias('month'),
                           year('datetime').alias('year'),
                           date_format('datetime','E').alias('weekday')
                           ).drop_duplicates() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data + 'time', mode="overwrite")
    
    print("successfully created time table in s3")

    # read in song data to use for songplays table
    song_data = input_data + "song-data/*/*/*/*.json"
    df_song = spark.read.json(song_data)

    df_song.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays table 
    time_table.createOrReplaceTempView("time")
    
    songplays_table = spark.sql('''
    SELECT
        row_number()  OVER (ORDER BY userId)AS songplay_id,
        t.start_time AS start_time, 
        l.userId AS user_id, 
        l.level AS level, 
        s.song_id AS song_id, 
        s.artist_id AS artist_id, 
        l.sessionId AS session_id, 
        l.location AS location, 
        l.userAgent AS user_agent,
        t.year AS year,
        t.month AS month
    FROM
        log_data l 
        JOIN song_data s ON
            l.song=s.title
            AND l.artist=s.artist_name
        JOIN time t ON
            l.ts=t.ts
    ''') 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + 'songplays', mode="overwrite")
    
    print("successfully songplays songs table in s3")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    #input_data = "./data/"
    #output_data = "./"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
