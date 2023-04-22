import configparser
import os
import sys
import boto3
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


def create_spark_session():
    ''' 
    This procedure creates the Spark session                
    '''    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    ''' 
    This procedure loads song data from the S3 input location
    and stores as parquet partitioned by year & artists_id
    
    INPUT PARAMETERS
        -- spark:       Apache Spark session
        -- input_data:  The path to the S3 song files
        -- output_data: The path where the output data will be stored    
    
    '''
    print ("Set song data file path",input_data)
    # set filepath to songs data file
    song_data = "{}song_data/*/*/*/*.json".format(input_data)    
    
    # set Schema to song data     
    
    # read song data file
    #song_dfs = spark.read.json(song_data, schema = )
    song_dfs = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_dfs.select(['artist_id','duration','song_id','title','year']).distinct()
        
    # write songs table to parquet files partitioned by year and artist    
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet("{}songs_table".format(output_data))
    
    # Create songs view
    songs_table.createOrReplaceTempView('songs')

    # extract columns to create artists table
    artist_table = song_dfs.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']).distinct()
    
    # Create artist view
    artist_table.createOrReplaceTempView('artist')

    # write artists table to parquet files
    artist_table.write.mode("overwrite").parquet("{}/artists_table".format(output_data))


def process_log_data(spark, input_data, output_data):
    ''' 
    This procedure loads json log data from the S3 input location
    and stores as parquet:
        The users table
        The time table partitioned by year & month
        The songplays table partitioned by year & month
        
    INPUT PARAMETERS
        -- spark:       Apache Spark session
        -- input_data:  The path to the S3 log files
        -- output_data: The path where the output data will be stored   
    
    '''

    # get filepath to log data file
    log_data = "{}log_data/*json".format(input_data)

    # read log data file
    log_dfs = spark.read.json(log_data)
    
    # filter by actions only for song plays
    log_dfs = log_dfs.where('page="NextSong"')
    
    # we need to create a primary key for songplay_id
    log_dfs = log_dfs.withColumn("songplay_id", monotonically_increasing_id()+1)

    # extract columns for users table    
    users_table =log_dfs.select(['userId','firstName','lastName','gender','level']).distinct()       
    
    # Create users view
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet("{}users_table".format(output_data))

    # create timestamp column from original timestamp column

    # set an udf function to extract timestamp
    #extract_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
    log_dfs = log_dfs.withColumn('timestamp',(log_dfs.ts.cast('float')/1000).cast("timestamp"))    
    
    # Create logs table View
    log_dfs.createOrReplaceTempView('logs')    
       
    # extract columns to create time table
    time_table = log_dfs.select(
    F.col("timestamp").alias("start_time"),    
    F.hour("timestamp").alias("hour"),
    F.minute("timestamp").alias("minute"),
    F.second("timestamp").alias("second"),
    F.dayofmonth("timestamp").alias("day"),
    F.month("timestamp").alias("month"),
    F.year("timestamp").alias("year"),    
    F.dayofweek("timestamp").alias("dayofweek"),
    F.weekofyear("timestamp").alias("week"),
    F.date_format(F.col("timestamp"),"E").alias("weekday")     
    )
    
    # sort by start time
    time_table = time_table.sort("start_time")
    
    # delete duplicates
    time_table = time_table.drop_duplicates(subset=['start_time'])
   
    # create time table View
    time_table.createOrReplaceTempView('timetable')
        
    # now write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'timetable'), partitionBy=['year', 'month'])
    
    # read in song data to use for songplays table
    song_data = "{}song_data/*/*/*/*.json".format(input_data)
    song_dfs = spark.read.json(song_data)
    songs_table = song_dfs.select(['artist_id','duration','song_id','title','year']).distinct()
   
    # Create songs view
    #songs_table.createOrReplaceTempView('songs')
    
    # extract columns from joined log, song and timetable views to create a songplays table     
    songplays_table = spark.sql("""
        SELECT l.songplay_id,
        l.ts AS start_time, 
        l.userId AS user_id,
        l.level,
        s.song_id,
        s.artist_id, 
        l.sessionId AS session_id,
        l.location,
        l.userAgent AS user_agent,
        t.year,
        t.month
        FROM logs l 
        JOIN songs s ON s.title=l.song
        JOIN timetable t ON l.timestamp=t.start_time
        """).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet("{}songplays_table".format(output_data))


def main():

    # Establish all the configuration parameters, such us AWS credentials and INPUT and OUTPUT paths
    print ("Leyendo fichero de configuración")
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    print ("Leído fichero de configuración") 
        
    os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')
    #os.environ['AWS_SESSION_TOKEN']=config.get('AWS','AWS_SESSION_TOKEN') # no more working with temporary Cloud Gateway keys :)
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'       
    # select if we are working local or in AWS, depending on the input parameters for the script
    if len(sys.argv)>1: # Cluster mode
        
        input_data  = config.get('AWS','INPUT_PATH') 
        output_data = config.get('AWS','OUTPUT_PATH')   
    
    else: # Local mode
    
        input_data  = config.get('LOCAL','INPUT_PATH') 
        output_data = config.get('LOCAL','OUTPUT_PATH')                  
            
    spark = create_spark_session()
    print ("Spark session created")
    
    print ("Starting song process")
    process_song_data(spark, input_data, output_data)    
    print ("Song data processed")
    process_log_data(spark, input_data, output_data)
    print ("Log data processed")
    

if __name__ == "__main__":
    main()
