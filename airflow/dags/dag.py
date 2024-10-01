import apifunctions.api as api
import apifunctions.dims as dims
import apifunctions.facts as facts
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Domingo Morelli',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


class ModdedPythonOperator(PythonOperator):
    template_fields = ('templates_dict', 'op_kwargs')


ds = '{{ ds }}'


with DAG(
    'music_dwh',
    default_args=default_args,
    description='',
    schedule_interval='00 16 * * *',
    catchup=False,
) as dag:

    extract_artists = ModdedPythonOperator(
        task_id='extract_artists',
        python_callable=api.extract_artists,
        op_kwargs={'today': ds}
    )

    etl_artist_data = ModdedPythonOperator(
        task_id='etl_artist_data',
        python_callable=api.etl_artist_data,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=7),
        op_kwargs={'table_name': 'staging_artists_daily', 'today': ds}
    )

    etl_track_data = ModdedPythonOperator(
        task_id='etl_track_data',
        python_callable=api.etl_track_data,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=7),
        op_kwargs={'table_name': 'staging_tracks_daily', 'today': ds}
    )

    etl_album_data = ModdedPythonOperator(
        task_id='etl_album_data',
        python_callable=api.etl_album_data,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=7),
        op_kwargs={'table_name': 'staging_albums_daily', 'today': ds}
    )

    artist_dim = ModdedPythonOperator(
        task_id='artist_dim',
        python_callable=dims.artist_dim,
        retry_delay=timedelta(minutes=2),
        op_kwargs={'table_name': 'dim_artists', 'today': ds}
    )

    tracks_dim = ModdedPythonOperator(
        task_id='tracks_dim',
        python_callable=dims.tracks_dim,
        retry_delay=timedelta(minutes=2),
        op_kwargs={'table_name': 'dim_tracks', 'today': ds}
    )

    albums_dim = ModdedPythonOperator(
        task_id='albums_dim',
        python_callable=dims.albums_dim,
        retry_delay=timedelta(minutes=2),
        op_kwargs={'table_name': 'dim_albums', 'today': ds}
    )

    track_fact = ModdedPythonOperator(
        task_id='track_fact',
        python_callable=facts.track_fact,
        retry_delay=timedelta(minutes=2),
        op_kwargs={'table_name': 'fact_tracks', 'today': ds}
    )

    album_fact = ModdedPythonOperator(
        task_id='album_fact',
        python_callable=facts.album_fact,
        retry_delay=timedelta(minutes=2),
        op_kwargs={'table_name': 'fact_albums', 'today': ds}
    )

extract_artists >> [etl_artist_data, etl_track_data, etl_album_data]
etl_artist_data >> artist_dim
etl_track_data >> tracks_dim
etl_album_data >> albums_dim
artist_dim >> [tracks_dim, albums_dim]
tracks_dim >> track_fact
albums_dim >> album_fact
