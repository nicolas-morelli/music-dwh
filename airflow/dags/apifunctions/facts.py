import os
import yaml
import logging
import redshift_connector
import awswrangler as wr


def from_redshift_to_redshift(func):
    def wrapper(*args, **kwargs):
        pathcreds = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env/.cfg', 'creds.yaml')

        with open(pathcreds, 'r') as creds:
            creds = yaml.safe_load(creds)
            host = creds['redshift']['host']
            port = creds['redshift']['port']
            db = creds['redshift']['db']
            user = creds['redshift']['user']
            password = creds['redshift']['password']
            logging.info('Credentials read.')

        logging.info('Connecting to Redshift.')
        conn = redshift_connector.connect(database=db, user=user, password=password, host=host, port=port)
        kwargs['conn'] = conn

        df_api = func(*args, **kwargs)
        df_api = df_api.infer_objects()

        table_name = kwargs['table_name']

        logging.info('Creating table.')
        wr.redshift.to_sql(df=df_api, con=conn, table=table_name, schema='2024_domingo_nicolas_morelli_schema', mode='append', use_column_names=True, lock=True, index=False)
        logging.info(f'{table_name} loaded.')

        return

    return wrapper


@from_redshift_to_redshift
def track_fact(*args, **kwargs):
    conn = kwargs['conn']

    with conn.cursor() as cur:
        cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_tracks_daily"')
        daily = cur.fetch_dataframe()

        logging.info('Fetched daily.')

        cur.execute("""SELECT DISTINCT track_name, track_id, artist_name
                    FROM "2024_domingo_nicolas_morelli_schema".dim_tracks t
                    JOIN (SELECT DISTINCT artist_id, artist_name FROM "2024_domingo_nicolas_morelli_schema".dim_artists) a ON a.artist_id = t.artist_id
                    """)
        artists = cur.fetch_dataframe().rename(columns={'track_name': 'name', 'artist_name': 'artist'})

        logging.info('Fetched id.')

    daily = daily.merge(artists, on=['name', 'artist'], how='inner').drop(['name', 'artist'], axis=1).drop_duplicates()

    logging.info('Data ready.')

    return daily


@from_redshift_to_redshift
def album_fact(*args, **kwargs):
    conn = kwargs['conn']

    with conn.cursor() as cur:
        cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_albums_daily"')
        daily = cur.fetch_dataframe()

        logging.info('Fetched daily.')

        cur.execute("""SELECT DISTINCT album_name, album_id, artist_name
                    FROM "2024_domingo_nicolas_morelli_schema"."dim_albums" t
                    JOIN (SELECT DISTINCT artist_id, artist_name FROM "2024_domingo_nicolas_morelli_schema".dim_artists) a ON a.artist_id = t.artist_id
                    """)
        artists = cur.fetch_dataframe().rename(columns={'album_name': 'name', 'artist_name': 'artist'})

        logging.info('Fetched id.')

    daily = daily.merge(artists, on=['name', 'artist'], how='inner').drop(['name', 'artist'], axis=1).drop_duplicates()

    logging.info('Data ready.')

    return daily
