from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

log_data_path = "s3://udacity-dend/log_data"
song_data_path = "s3://udacity-dend/song_data"

default_args = {
    'owner': 'Camilo Montenegro',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past':False,
    'catchup' : False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('pipeline_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    source = log_data_path,
    destination = "staging_events",
    access_key = AWS_KEY,
    secret_key = AWS_SECRET,
    region = "us-west-2",
    appendData=False
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    source = log_data_path,
    destination = "staging_songs",
    access_key = AWS_KEY,
    secret_key = AWS_SECRET,
    region = "us-west-2",
    appendData=False
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt = SqlQueries.songplay_table_insert,
    destination = "songplays",
    atributes = "playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent",
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt = SqlQueries.user_table_insert,
    destination = "users",
    atributes = "userid, first_name, last_name, gender, level",
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt = SqlQueries.song_table_insert,
    destination = "songs",
    atributes = "songid, title, artistid, year, duration",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt = SqlQueries.artist_table_insert,
    destination = "artist",
    atributes = "artistid, name, location, lattitude, longitude",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt = SqlQueries.time_table_insert,
    destination = "time",
    atributes = "start_time, hour, day, week, month, year, weekday",
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables = ['songplays','artists','songs','time','users']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
