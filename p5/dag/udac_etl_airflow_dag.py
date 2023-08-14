from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Default arguments for DAG 
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,    
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_retry': False
}

# DAG object definition
dag = DAG('udac_etl_airflow_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *'
          schedule_interval='@daily',
          catchup=False
        )

# Task to start
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Task to copy all the log data from the S3 bucket to the Redshift 
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id="redhift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

# Task to copy song data from the S3 bucket to Redshift 
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    extra_params="JSON 'auto' COMPUDATE OFF"
)

# Task to load songplays fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.songplay_table_insert
)

# Task to load user dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.user_table_insert    
)
# Task to load song dimension table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.song_table_insert
)

# Task to load artist dimension table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artist',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.artist_table_insert
)
# Task to load time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.time_table_insert
)

# Task to run some quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(*) FROM public.time WHERE weekday IS NULL', 'expected_result': 0 },                 { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 }       
    ],
    redshift_conn_id="redshift"
)

# Task to end the execution
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Now let's order the tasks, so that they will be executed in the right order
# First we have to load songs and events to our Redshift DWH
start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift

# After loading songs and events table, then we can load the songplays_table
stage_songs_to_redshift  >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

# And now we can load the dimensional tables
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# After loading the dimensional tables, we launch some quality checks
load_user_dimension_table   >> run_quality_checks
load_song_dimension_table   >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table   >> run_quality_checks

# And once the quality checks have been finished, then we can finish
run_quality_checks >> end_operator




