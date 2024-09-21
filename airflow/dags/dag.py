import apifunctions.api as api
import apifunctions.dims as dims
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Domingo Morelli',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'music_dwh',
    default_args=default_args,
    description='',
    schedule_interval='00 16 * * *',
    catchup=False,
) as dag:

    extract_artists = PythonOperator(
        task_id='extract_artists',
        python_callable=api.extract_artists
    )

    etl_artist_data = PythonOperator(
        task_id='etl_artist_data',
        python_callable=api.etl_artist_data,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=7),
        op_kwargs={'table_name': 'staging_artists_daily'}
    )

    etl_track_data = PythonOperator(
        task_id='etl_track_data',
        python_callable=api.etl_track_data,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=7),
        op_kwargs={'table_name': 'staging_tracks_daily'}
    )

    etl_album_data = PythonOperator(
        task_id='etl_album_data',
        python_callable=api.etl_album_data,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=7),
        op_kwargs={'table_name': 'staging_albums_daily'}
    )

    artist_dim = PythonOperator(
        task_id='artist_dim',
        python_callable=dims.artist_dim,
        retry_delay=timedelta(minutes=2),
        op_kwargs={'table_name': 'dim_artists'}
    )

    tracks_dim = PythonOperator(
        task_id='tracks_dim',
        python_callable=dims.tracks_dim,
        retry_delay=timedelta(minutes=2),
        op_kwargs={'table_name': 'dim_tracks'}
    )

extract_artists >> [etl_artist_data, etl_track_data, etl_album_data]
etl_artist_data >> artist_dim
etl_track_data >> tracks_dim
artist_dim >> tracks_dim
