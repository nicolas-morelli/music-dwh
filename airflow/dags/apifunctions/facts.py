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

        conn = redshift_connector.connect(database=db, user=user, password=password, host=host, port=port)
        kwargs['conn'] = conn

        df_api = func(*args, **kwargs)
        df_api = df_api.infer_objects()

        table_name = kwargs['table_name']
        wr.redshift.to_sql(df=df_api, con=conn, table=table_name, schema='2024_domingo_nicolas_morelli_schema', mode='append', use_column_names=True, lock=True, index=False)
        logging.info(f'{table_name} loaded.')

        return

    return wrapper


@from_redshift_to_redshift
def artist_fact(*args, **kwargs):
    conn = kwargs['conn']

    with conn.cursor() as cur:
        cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_artists_daily"')
        daily = cur.fetch_dataframe()
        cur.execute('SELECT DISTINCT artist_name, artist_id, artist_tag FROM "2024_domingo_nicolas_morelli_schema"."dim_artists"')
        artists = cur.fetch_dataframe().rename(columns={'artist_name': 'name', 'artist_tag': 'tag'})

    daily = daily.merge(artists, on=['name', 'tag'], how='inner').drop(['name', 'tag'], axis=1)

    return daily


@from_redshift_to_redshift
def track_fact(*args, **kwargs):
    conn = kwargs['conn']

    with conn.cursor() as cur:
        cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_tracks_daily"')
        daily = cur.fetch_dataframe()
        cur.execute('SELECT DISTINCT track_name, track_id FROM "2024_domingo_nicolas_morelli_schema"."dim_tracks"')
        artists = cur.fetch_dataframe().rename(columns={'track_name': 'name'})

    daily = daily.merge(artists, on='name', how='inner').drop(['name', 'artist'], axis=1)

    return daily


@from_redshift_to_redshift
def album_fact(*args, **kwargs):
    conn = kwargs['conn']

    with conn.cursor() as cur:
        cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_albums_daily"')
        daily = cur.fetch_dataframe()
        cur.execute('SELECT DISTINCT album_name, album_id FROM "2024_domingo_nicolas_morelli_schema"."dim_albums"')
        artists = cur.fetch_dataframe().rename(columns={'album_name': 'name'})

    daily = daily.merge(artists, on='name', how='inner').drop(['name', 'artist'], axis=1)

    return daily
