from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,LoadFactOperator, LoadDimensionOperator
,DataQualityOperator,TestDataOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'rasha',
    'email_on_retry':False,
    'start_date': datetime(2020, 9, 24),
    'end_date': datetime(2020 , 9, 25),
    'Depends_on_past':False,
    'retries': 3,
    'retry_delay':timedelta(minutes=5),
    'catchup':False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

"""
    First we need to create tables in redshift cluster
"""
start_operator = PostgresOperator(task_id='Begin_execution', 
                                 dag=dag, 
                                 postgres_conn_id = 'redshift',
                                 sql='/create_tables.sql')

#  staging events to redshift using StageToRedshiftOperator -- I staged a subset of the data you can stage specific data using  s3_key="log_data/{execution_date.year}/{execution_date.month}/
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/2018-11-25-events.json",
    provider_context = True
)

#  staging songs to redshift using StageToRedshiftOperator -- I staged a subset of songs data you can stage all data using  s3_key="song_data/A/A/
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song-data/A/A/A/"
)

# load staged data into songplays Fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_stmt=SqlQueries.songplay_table_insert
    
)

# load Dimension tables from staged data
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_stmt=SqlQueries.user_table_insert,
    update_mode="insert",
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_stmt=SqlQueries.song_table_insert,
    update_mode="insert",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_stmt=SqlQueries.artist_table_insert,
    update_mode="insert",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_stmt=SqlQueries.time_table_insert,
    update_mode="insert",
)

# check songplays have rows
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
)

# check that songsplays doesnot have Null values in playid column
run_case_quality_checks = TestDataOperator(
    task_id='Run_data_case_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    sql="select COUNT(*) from songplays where playid is NULL ",
    expected = 0
)

# just a dummy operator execute nothing
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Dag Tasks Dependecies
start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]
run_quality_checks << [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]
run_quality_checks >> run_case_quality_checks

run_case_quality_checks >> end_operator