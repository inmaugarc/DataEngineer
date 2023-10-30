# Following the Udacity honour code: I have used the Udacity templates as a base for these scripts
# I have used some pieces of code of the Udacity exercises and lessons and some from my previous Udacity projects (like the create_redshift)
# Also I may have used some ideas from the Udacity knowledge area and from stackoverflow to solve the errors I had

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators import StageToRedshiftOperator --> it throws an error
from operators.s3_to_redshift import StageToRedshiftOperator
#from airflow.operators import LoadFactOperator --> it throws an error
from operators.LoadFactOperator import LoadFactOperator
#from airflow.operators import LoadDimensionOperator --> it throws an error
from operators.LoadDimensionOperator import LoadDimensionOperator
#from airflow.operators import DataQualityOperator --> it throws an error
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries

# Default arguments for DAG according to the guidelines:
'''
The DAG does not have dependencies on past runs
On failure, the task are retried 3 times
Retries happen every 5 minutes
Catchup is turned off
Do not email on retry
'''
default_args = {
    'owner': 'inma',
    'depends_on_past': False, # The DAG does not have dependencies on past runs
    'start_date': datetime(2019, 1, 12),
    'retries': 3,    # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5), # Retries happen every 5 minutes 
    'email_on_retry': False # Do not email on retry
}

# DAG configuration that meets Project Rubric
dag = DAG('udac_etl_airflow_dag',
          default_args=default_args,
          description='Load and transform data from an S3 bucket to Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
        )

# Task to start
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Task to copy all the log data from the S3 bucket to the Redshift 
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag, 
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket="udacity-dend",
    s3_key="log_data",
    jsonpath="s3://udacity-dend/log_json_path.json"     
)

# Task to copy song data from the S3 bucket to Redshift 
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)

# A task using the fact load operator is in the DAG
# Facts are loaded with on the LoadFact operator:
# Task to load songplays fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    load_sql_stmt=SqlQueries.songplay_table_insert
)

# Task to load user dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users', 
    truncate_table=True,
    load_sql_stmt=SqlQueries.user_table_insert    
)

# Set of tasks using the dimension load operator is in the DAG
# Dimensions are loaded with on the LoadDimension operator

# Task to load song dimension table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    truncate_table=True,
    load_sql_stmt=SqlQueries.song_table_insert
)

# Task to load artist dimension table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    truncate_table=True,
    load_sql_stmt=SqlQueries.artist_table_insert
)
# Task to load time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    truncate_table=True,
    load_sql_stmt=SqlQueries.time_table_insert
)

# Task to run some quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 },   
        { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(*) FROM public.time WHERE weekday IS NULL', 'expected_result': 0 }    
    ],
    redshift_conn_id='redshift'
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





