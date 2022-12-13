from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

sql_queries = SqlQueries()
 
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'fred.waldow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 2),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup_by_default': False 
}

dag = DAG('sparkify_etl',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}',
    json_format='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    sql_query=sql_queries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    sql_query=sql_queries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    sql_query=sql_queries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    sql_query=sql_queries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    sql_query=sql_queries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks = [
        {'test_query': 'SELECT COUNT(*) FROM staging_events',
            'operator': 'greater_than',
            'value': 0 },
        {'test_query': 'SELECT COUNT(*) FROM staging_songs',
            'operator': 'greater_than',
            'value': 0 },
        {'test_query': 'SELECT COUNT(*) FROM songplays',
            'operator': 'greater_than',
            'value': 0 },
        {'test_query': 'SELECT COUNT(*) FROM users',
            'operator': 'greater_than',
            'value': 0 },
        {'test_query': 'SELECT COUNT(*) FROM songs',
            'operator': 'greater_than',
            'value': 0 },
        {'test_query': 'SELECT COUNT(*) FROM artists',
            'operator': 'greater_than',
            'value': 0 },
        {'test_query': 'SELECT COUNT(*) FROM time',
            'operator': 'greater_than',
            'value': 0 }
    ]
    tables_with_pk = {
        'staging_events': 1, # table has no primary key
        'staging_songs': 1, # table has no primary key
        'songplays': 'songplay_id',
        'users': 'userid',
        'songs': 'song_id',
        'artists': 'artist_id',
        'time': 1 # table has no primary key
    }
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Setting DAG operation order
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [
    load_song_dimension_table, 
    load_artist_dimension_table, 
    load_time_dimension_table, 
    load_user_dimension_table
]

[
    load_song_dimension_table, 
    load_artist_dimension_table, 
    load_time_dimension_table, 
    load_user_dimension_table
] >> run_quality_checks

run_quality_checks >> end_operator

