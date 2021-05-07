from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateRedshiftTablesOperator)
from helpers import SqlQueries, CreateTables

S3_SONG_KEY = Variable.get('S3_SONG_KEY')
S3_LOG_KEY = Variable.get('S3_LOG_KEY')
S3_BUCKET = Variable.get('S3_BUCKET')

default_args = {
    'owner': 'Bank',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

songplays_table_create = CreateRedshiftTablesOperator(
    task_id = 'create_songplays_table',
    dag = dag,
    table = 'songplays',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTables.songplays_table_create
)

artists_table_create = CreateRedshiftTablesOperator(
    task_id = 'create_artist_table',
    dag = dag,
    table = 'artists',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTables.artists_table_create
)

songs_table_create = CreateRedshiftTablesOperator(
    task_id = 'create_songs_table',
    dag = dag,
    table = 'songs',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTables.songs_table_create
)

users_table_create = CreateRedshiftTablesOperator(
    task_id = 'create_users_table',
    dag = dag,
    table = 'users',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTables.users_table_create
)

time_table_create = CreateRedshiftTablesOperator(
    task_id = 'create_times_table',
    dag = dag,
    table = 'times',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTables.time_table_create
)

staging_events_table_create = CreateRedshiftTablesOperator(
    task_id = 'create_staging_events_table',
    dag = dag,
    table = 'staging_events',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTables.staging_events_table_create
)

staging_songs_table_create = CreateRedshiftTablesOperator(
    task_id = 'create_staging_songs_table',
    dag = dag,
    table = 'staging_songs',
    redshift_conn_id = 'redshift',
    create_table_sql = CreateTables.staging_songs_table_create
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = 'staging_events',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials_id',
    s3_key = S3_LOG_KEY,
    s3_bucket = S3_BUCKET
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    table = 'staging_songs',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials_id',
    s3_key = S3_SONG_KEY,
    s3_bucket = S3_BUCKET
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = 'songplays',
    fields = 'playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent',
    redshift_conn_id = 'redshift',
    load_facts_sql = SqlQueries.songplay_table_insert,
    
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    table = 'users',
    fields = 'userid, first_name, last_name, gender, level',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    table = 'songs',
    fields = 'songid, title, artistid, year, duration ',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    table = 'artists',
    fields = 'artistid, name, location, lattitude, longitude',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    table = 'time',
    fields = 'start_time, hour, day, week, month, year, week_day',
    redshift_conn_id = 'redshift',
    load_dimension = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    tables = ['songplays', 'songs', 'artists', 'users', 'time'],
    redshift_conn_id = 'redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> songplays_table_create
start_operator >> artists_table_create
start_operator >> songs_table_create
start_operator >> users_table_create
start_operator >> time_table_create
start_operator >> staging_events_table_create
start_operator >> staging_songs_table_create

songplays_table_create >> stage_events_to_redshift
artists_table_create >> stage_events_to_redshift
songs_table_create >> stage_events_to_redshift
users_table_create >> stage_events_to_redshift
time_table_create >> stage_events_to_redshift
staging_events_table_create >> stage_events_to_redshift
staging_songs_table_create >> stage_events_to_redshift

songplays_table_create >> stage_songs_to_redshift
artists_table_create >> stage_songs_to_redshift
songs_table_create >> stage_songs_to_redshift
users_table_create >> stage_songs_to_redshift
time_table_create >> stage_songs_to_redshift

staging_events_table_create >> stage_songs_to_redshift
staging_songs_table_create >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator