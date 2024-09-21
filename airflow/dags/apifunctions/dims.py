import os
import yaml
import logging
from datetime import datetime
import redshift_connector
import awswrangler as wr
import pandas as pd


def handle_new(daily, type):
    daily = daily.rename(columns={'name': 'artist_name', 'rank': 'current_rank', 'stats_date': 'effective_date'})
    daily['max_rank'] = daily['current_rank']
    daily['new_listeners'] = daily['listeners']
    daily['new_plays'] = daily['playcount']
    daily['expiration_date'] = '9999-12-31'
    daily['last_known'] = 'Yes'

    if type == 'artists':
        daily['consecutive_times_in_top_50'] = 1

    return daily


def handle_repeated(daily, type):
    daily['new_plays'] = daily['playcount_daily'].astype(int) - daily['playcount_old'].astype(int).fillna(0)
    daily['playcount'] = daily['playcount_daily'].astype(int)
    daily['new_listeners'] = daily['listeners_daily'].astype(int) - daily['listeners_old'].astype(int).fillna(0)
    daily['listeners'] = daily['listeners_daily'].astype(int)
    daily['max_rank'] = daily[['current_rank_daily', 'max_rank']].astype(int).min(axis=1)
    daily['current_rank'] = daily['current_rank_daily'].astype(int)
    daily['expiration_date'] = '9999-12-31'
    daily['effective_date'] = daily['effective_date_daily']
    daily['artist_tag'] = daily['artist_tag_daily']

    if type == 'artists':
        daily['consecutive_times_in_top_50'] += 1

    cols_to_drop = []
    for col in daily.columns:
        if '_daily' in col or '_old' in col:
            cols_to_drop.append(col)
    cols_to_drop.append('id')

    daily = daily.drop(cols_to_drop, axis=1)

    return daily


def handle_out(daily, type):
    daily['new_listeners'] = 0
    daily['new_plays'] = 0
    daily['current_rank'] = pd.NA
    daily = daily.drop('id', axis=1)

    return daily


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


# TODO: Cuando ya haya pensado logica, sumarle TOP track, TOP album y tag tal vez
@from_redshift_to_redshift
def artist_dim(*args, **kwargs):
    conn = kwargs['conn']
    table_name = kwargs['table_name']

    try:
        with conn.cursor() as cur:
            cur.execute(f"""

                        SELECT *
                        FROM "2024_domingo_nicolas_morelli_schema"."{table_name}"
                        WHERE last_known = 'Yes'

                        """)
            artists = cur.fetch_dataframe()

            cur.execute(f"""UPDATE "2024_domingo_nicolas_morelli_schema"."{table_name}"
                            SET last_known = 'No',
                                expiration_date = {datetime.now().strftime('%Y-%m-%d')}
                            WHERE last_known = 'Yes' AND artist_name IN (SELECT name FROM "2024_domingo_nicolas_morelli_schema"."staging_artists_daily")
                        """)
            conn.commit()

            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_artists_daily"')
            daily = cur.fetch_dataframe()

            daily = daily.rename(columns={'name': 'artist_name', 'rank': 'current_rank', 'stats_date': 'effective_date', 'tag': 'artist_tag'})

            daily_new_artists = daily[~daily['artist_name'].isin(artists['artist_name'])]
            daily_repeated_artists = daily.merge(artists, on='artist_name', how='inner', suffixes=['_daily', '_old'])
            daily_out_artists = artists[~artists['artist_name'].isin(daily['artist_name'])]

            daily_new_artists = handle_new(daily_new_artists, type='artists')
            daily_repeated_artists = handle_repeated(daily_repeated_artists, type='artists')
            daily_out_artists = handle_out(daily_out_artists, type='artists')

            daily_new_artists['artist_id'] = pd.NA

            daily = pd.concat([daily_new_artists, daily_repeated_artists, daily_out_artists]).reset_index(drop=True)

            max_id = daily['artist_id'].fillna(-1).astype(int).max()

            for index, _ in daily[daily['artist_id'].isna()].iterrows():
                daily.loc[index, 'artist_id'] = max_id + 1
                max_id += 1

    except redshift_connector.error.ProgrammingError:
        with conn.cursor() as cur:
            conn.commit()
            cur.execute(f"""
                            CREATE TABLE "2024_domingo_nicolas_morelli_schema"."{table_name}"
                            (
                              id INTEGER IDENTITY(1, 1),
                              artist_id INTEGER,
                              artist_name VARCHAR,
                              artist_tag VARCHAR,
                              max_rank INTEGER,
                              current_rank INTEGER,
                              listeners INTEGER,
                              new_listeners INTEGER,
                              playcount INTEGER,
                              new_plays INTEGER,
                              consecutive_times_in_top_50 INTEGER,
                              effective_date VARCHAR,
                              expiration_date VARCHAR,
                              last_known VARCHAR
                            )

                        """)
            conn.commit()
            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_artists_daily"')

            daily = cur.fetch_dataframe()

            daily = handle_new(daily, type='artists')
            daily = daily.drop('tag', axis=1).drop_duplicates().reset_index(names='artist_id')

    return daily


@from_redshift_to_redshift
def tracks_dim(*args, **kwargs):
    conn = kwargs['conn']
    table_name = kwargs['table_name']

    try:
        with conn.cursor() as cur:
            cur.execute(f"""

                        SELECT *
                        FROM "2024_domingo_nicolas_morelli_schema"."{table_name}"
                        WHERE last_known = 'Yes'

                        """)
            tracks = cur.fetch_dataframe()

            cur.execute(f"""UPDATE "2024_domingo_nicolas_morelli_schema"."{table_name}"
                            SET last_known = 'No',
                                expiration_date = {datetime.now().strftime('%Y-%m-%d')}
                            WHERE last_known = 'Yes' AND track_name || artist_id IN (SELECT DISTINCT dt.name || CAST(da.artist_id AS VARCHAR(255)) FROM "2024_domingo_nicolas_morelli_schema"."staging_tracks_daily" dt JOIN "2024_domingo_nicolas_morelli_schema"."{table_name}" da ON da.artist_name = dt.artist)
                        """)
            conn.commit()

            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_tracks_daily"')
            daily = cur.fetch_dataframe()

            daily = daily.rename(columns={'name': 'track_name', 'rank': 'current_rank', 'stats_date': 'effective_date'})

            daily_new_tracks = daily[~daily['track_name'].isin(tracks['track_name'])]
            daily_repeated_tracks = daily.merge(tracks, on='track_name', how='inner', suffixes=['_daily', '_old'])
            daily_out_tracks = tracks[~tracks['track_name'].isin(daily['track_name'])]

            daily_new_tracks = handle_new(daily_new_tracks, type='tracks')
            daily_repeated_tracks = handle_repeated(daily_repeated_tracks, type='tracks')
            daily_out_tracks = handle_out(daily_out_tracks, type='tracks')

            daily_new_tracks['track_id'] = pd.NA

            daily = pd.concat([daily_new_tracks, daily_repeated_tracks, daily_out_tracks]).reset_index(drop=True)

            max_id = daily['track_id'].fillna(-1).astype(int).max()

            for index, _ in daily[daily['track_id'].isna()].iterrows():
                daily.loc[index, 'track_id'] = max_id + 1
                max_id += 1

    except redshift_connector.error.ProgrammingError:
        with conn.cursor() as cur:
            conn.commit()
            cur.execute(f"""
                            CREATE TABLE "2024_domingo_nicolas_morelli_schema"."{table_name}"
                            (
                              id INTEGER IDENTITY(1, 1),
                              track_id INTEGER,
                              artist_id INTEGER,
                              track_name VARCHAR,
                              max_rank INTEGER,
                              current_rank INTEGER,
                              listeners INTEGER,
                              new_listeners INTEGER,
                              playcount INTEGER,
                              new_plays INTEGER,
                              effective_date VARCHAR,
                              expiration_date VARCHAR,
                              last_known VARCHAR
                            )

                        """)
            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_tracks_daily"')

            daily = cur.fetch_dataframe()

            daily = handle_new(daily, type='tracks')
            daily = daily.drop_duplicates().reset_index(names='track_id')

        with conn.cursor() as cur:
            cur.execute('SELECT DISTINCT artist_name, artist_id FROM "2024_domingo_nicolas_morelli_schema"."dim_artists"')
            artists = cur.fetch_dataframe().rename(columns={'artist_name': 'artist'})

        daily = daily.merge(artists, on='artist', how='inner').drop('artist', axis=1)

    return daily


def tag_dim(*args, **kwargs):
    # Tags
    # TODO: Pruebo consulta y sino creo la tabla
    # TODO: Me fijo los artistas, si hay alguno nuevo lo agrego y le creo un ID
    # TODO: Con SCD 2 actualizo los dias viejos y creo el nuevo con los datos del momento
    # Totalidad de escuchas, promedio de listeners del top 50, artista actual en el rank 1, cancion de algun artista del top 50 con mas escuchas
    pass
