import os
import logging
import redshift_connector
import awswrangler as wr
import pandas as pd
from dotenv import load_dotenv


def from_redshift_to_redshift(func):
    """Decorator for loading database info and appending new data to tables.

    :param func: Function that returns a Pandas dataframe with new rows.
    :type func: function
    """
    def wrapper(*args, **kwargs):
        pathcreds = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env')

        load_dotenv(pathcreds)
        host = os.getenv('REDSHIFT_HOST')
        port = os.getenv('REDSHIFT_PORT')
        db = os.getenv('REDSHIFT_DB')
        schema = os.getenv('REDSHIFT_SCHEMA')
        user = os.getenv('REDSHIFT_USER')
        password = os.getenv('REDSHIFT_PW')
        logging.info('Credentials read.')

        logging.info('Connecting to Redshift.')
        conn = redshift_connector.connect(database=db, user=user, password=password, host=host, port=port)
        kwargs['conn'] = conn
        kwargs['schema'] = schema

        df_api = func(*args, **kwargs)
        df_api = df_api.infer_objects()

        table_name = kwargs['table_name']

        logging.info('Creating table.')
        wr.redshift.to_sql(df=df_api, con=conn, table=table_name, schema=schema, mode='append', use_column_names=True, lock=True, index=False)
        logging.info(f'{table_name} loaded.')

        return

    return wrapper


@from_redshift_to_redshift
def track_fact(*args, **kwargs) -> pd.DataFrame:
    """Generates track fact in Pandas dataframe

    :return: Track fact data.
    :rtype: pd.DataFrame
    """
    conn = kwargs['conn']
    schema = kwargs['schema']

    with conn.cursor() as cur:
        cur.execute(f"""SELECT * FROM "{schema}"."staging_tracks_daily" WHERE stats_date = '{kwargs['today']}'""")
        daily = cur.fetch_dataframe()

        logging.info('Fetched daily.')

        cur.execute(f"""SELECT DISTINCT track_name, track_id, artist_name, t.artist_id
                    FROM "{schema}".dim_tracks t
                    JOIN (SELECT DISTINCT artist_id, artist_name FROM "{schema}".dim_artists) a ON a.artist_id = t.artist_id
                    """)
        artists = cur.fetch_dataframe().rename(columns={'track_name': 'name', 'artist_name': 'artist'})

        logging.info('Fetched id.')

    daily = daily.merge(artists, on=['name', 'artist'], how='inner').drop(['name', 'artist'], axis=1).drop_duplicates()

    logging.info('Data ready.')

    return daily


@from_redshift_to_redshift
def album_fact(*args, **kwargs) -> pd.DataFrame:
    """Generates album fact in Pandas dataframe

    :return: Album fact data.
    :rtype: pd.DataFrame
    """
    conn = kwargs['conn']
    schema = kwargs['schema']

    with conn.cursor() as cur:
        cur.execute(f"""SELECT * FROM "{schema}"."staging_albums_daily" WHERE stats_date = '{kwargs['today']}'""")
        daily = cur.fetch_dataframe()

        logging.info('Fetched daily.')

        cur.execute(f"""SELECT DISTINCT album_name, album_id, artist_name, t.artist_id
                    FROM "{schema}"."dim_albums" t
                    JOIN (SELECT DISTINCT artist_id, artist_name FROM "{schema}".dim_artists) a ON a.artist_id = t.artist_id
                    """)
        artists = cur.fetch_dataframe().rename(columns={'album_name': 'name', 'artist_name': 'artist'})

        logging.info('Fetched id.')

    daily = daily.merge(artists, on=['name', 'artist'], how='inner').drop(['name', 'artist'], axis=1).drop_duplicates()

    logging.info('Data ready.')

    return daily
