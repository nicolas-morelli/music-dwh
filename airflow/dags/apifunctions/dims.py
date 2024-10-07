import os
import logging
import redshift_connector
import awswrangler as wr
import pandas as pd
from dotenv import load_dotenv


def handle_new(daily: pd.DataFrame, type: str) -> pd.DataFrame:
    """Handles new entries in top 50s.

    :param daily: Dataframe of new entries.
    :type daily: pd.DataFrame
    :param type: Type of data, for specific transformations
    :type type: str
    :return: Transformed data of new entries.
    :rtype: pd.DataFrame
    """
    if type == 'album':
        daily = daily.rename(columns={'name': type + '_name', 'stats_date': 'effective_date'})
        daily['new_plays'] = daily['playcount']
    else:
        daily = daily.rename(columns={'name': type + '_name', 'rank': 'current_rank', 'stats_date': 'effective_date'})
        daily['max_rank'] = daily['current_rank']
        daily['new_listeners'] = daily['listeners']
        daily['new_plays'] = daily['playcount']

    if type == 'artist':
        daily['consecutive_times_in_top_50'] = 1

    daily['expiration_date'] = '9999-12-31'
    daily['last_known'] = 'Yes'

    logging.info(f'New {type} handled.') if not daily.empty else logging.info(f'No new {type}.')

    return daily


def handle_repeated(daily: pd.DataFrame, type: str) -> pd.DataFrame:
    """Handles repeated entries in top 50s.

    :param daily: Dataframe of repeated entries.
    :type daily: pd.DataFrame
    :param type: Type of data, for specific transformations
    :type type: str
    :return: Transformed data of repeated entries.
    :rtype: pd.DataFrame
    """
    if type == 'album':
        daily['new_plays'] = daily['playcount_daily'].astype(int) - daily['playcount_old'].astype(int).fillna(0)
        daily['playcount'] = daily['playcount_daily'].astype(int)
    else:
        daily['new_plays'] = daily['playcount_daily'].astype(int) - daily['playcount_old'].astype(int).fillna(0)
        daily['playcount'] = daily['playcount_daily'].astype(int)
        daily['new_listeners'] = daily['listeners_daily'].astype(int) - daily['listeners_old'].astype(int).fillna(0)
        daily['listeners'] = daily['listeners_daily'].astype(int)
        daily['max_rank'] = daily[['current_rank_daily', 'max_rank']].astype(int).min(axis=1)
        daily['current_rank'] = daily['current_rank_daily'].astype(int)

    daily['expiration_date'] = '9999-12-31'
    daily['effective_date'] = daily['effective_date_daily']

    if type == 'artist':
        daily['consecutive_times_in_top_50'] += 1

    cols_to_drop = []
    for col in daily.columns:
        if '_daily' in col or '_old' in col:
            cols_to_drop.append(col)
    cols_to_drop.append('id')

    daily = daily.drop(cols_to_drop, axis=1)

    logging.info(f'Repeated {type} handled.') if not daily.empty else logging.info(f'No repeated {type}.')

    return daily


def handle_out(daily: pd.DataFrame, type: str, today: str) -> pd.DataFrame:
    """Handles leaving entries in top 50s.

    :param daily: Dataframe of leaving entries.
    :type daily: pd.DataFrame
    :param type: Type of data, for specific transformations
    :type type: str
    :param today: Date of execution
    :type today: str
    :return: Transformed data of leaving entries.
    :rtype: pd.DataFrame
    """
    daily['new_plays'] = 0

    if not type == 'album':
        daily['new_listeners'] = 0
        daily['current_rank'] = pd.NA

    daily['effective_date'] = today

    daily = daily.drop('id', axis=1)

    logging.info(f'Leaving {type} handled.') if not daily.empty else logging.info(f'No leaving {type}.')

    return daily


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
        user = os.getenv('REDSHIFT_USER')
        password = os.getenv('REDSHIFT_PW')
        logging.info('Credentials read.')

        logging.info('Connecting to Redshift.')

        conn = redshift_connector.connect(database=db, user=user, password=password, host=host, port=port)
        kwargs['conn'] = conn

        df_api = func(*args, **kwargs)
        df_api = df_api.infer_objects()

        table_name = kwargs['table_name']

        logging.info('Creating table.')
        wr.redshift.to_sql(df=df_api, con=conn, table=table_name, schema='2024_domingo_nicolas_morelli_schema', mode='append', use_column_names=True, lock=True, index=False, chunksize=1000)
        logging.info(f'{table_name} loaded.')

        return

    return wrapper


@from_redshift_to_redshift
def artist_dim(*args, **kwargs) -> pd.DataFrame:
    """Generates artists dimension in Pandas dataframe

    :return: Artist dimension data.
    :rtype: pd.DataFrame
    """
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

            logging.info('Fetched last known data.')

            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_artists_daily"')
            daily = cur.fetch_dataframe()

            logging.info('Fetched daily data.')

            daily = daily.rename(columns={'name': 'artist_name', 'rank': 'current_rank', 'stats_date': 'effective_date', 'tag': 'artist_tag'})

            daily_new_artists = daily[~daily['artist_name'].isin(artists['artist_name'])]
            daily_repeated_artists = daily.merge(artists, on=['artist_name', 'artist_tag'], how='inner', suffixes=['_daily', '_old'])
            daily_out_artists = artists[(~artists['artist_name'].isin(daily['artist_name'])) & (~artists['current_rank'].isna())]

            logging.info('Divided data.')

            daily_new_artists = handle_new(daily_new_artists, type='artist')
            daily_repeated_artists = handle_repeated(daily_repeated_artists, type='artist')
            daily_out_artists = handle_out(daily_out_artists, type='artist', today=kwargs['today'])

            logging.info('Handled data.')

            daily_new_artists['artist_id'] = pd.NA

            daily = pd.concat([daily_new_artists, daily_repeated_artists, daily_out_artists]).drop_duplicates().reset_index(drop=True)

            logging.info('Concatenated data.')

            cur.execute(f'SELECT MAX(artist_id) FROM "2024_domingo_nicolas_morelli_schema"."{table_name}"')

            max_id = cur.fetch_dataframe().iloc[0, 0]

            for index, _ in daily[daily['artist_id'].isna()].iterrows():
                daily.loc[index, 'artist_id'] = max_id + 1
                max_id += 1

            logging.info('New ids generated.')

            cur.execute(f"""UPDATE "2024_domingo_nicolas_morelli_schema"."{table_name}"
                            SET last_known = 'No',
                                expiration_date = '{kwargs['today']}'
                            WHERE last_known = 'Yes'
                                AND (current_rank IS NOT NULL
                                    OR artist_name IN (SELECT name FROM "2024_domingo_nicolas_morelli_schema"."staging_artists_daily"))
                        """)
            conn.commit()

            logging.info('Updated previous last known.')

    except redshift_connector.error.ProgrammingError:
        with conn.cursor() as cur:
            logging.info('Table not found. Creating.')
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

            logging.info('Fetched daily data.')

            daily = daily.rename(columns={'name': 'artist_name', 'rank': 'current_rank', 'stats_date': 'effective_date', 'tag': 'artist_tag'})

            daily = handle_new(daily, type='artist')
            daily = daily.drop_duplicates().reset_index(names='artist_id')

            logging.info('Handled new data.')

    logging.info('Data ready.')
    return daily


@from_redshift_to_redshift
def tracks_dim(*args, **kwargs) -> pd.DataFrame:
    """Generates track dimension in Pandas dataframe

    :return: Track dimension data.
    :rtype: pd.DataFrame
    """
    conn = kwargs['conn']
    table_name = kwargs['table_name']

    try:
        with conn.cursor() as cur:
            cur.execute(f"""

                            SELECT tn.*, a.artist_name AS artist
                            FROM "2024_domingo_nicolas_morelli_schema"."{table_name}" tn
                            JOIN (SELECT DISTINCT artist_id, artist_name FROM "2024_domingo_nicolas_morelli_schema".dim_artists) a ON a.artist_id = tn.artist_id
                            WHERE tn.last_known = 'Yes'

                        """)
            tracks = cur.fetch_dataframe()

            logging.info('Fetched last known data.')

            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_tracks_daily"')
            daily = cur.fetch_dataframe()

            logging.info('Fetched daily data.')

            daily = daily.rename(columns={'name': 'track_name', 'rank': 'current_rank', 'stats_date': 'effective_date'})

            daily['concat'] = daily['track_name'] + daily['artist']
            tracks['concat'] = tracks['track_name'] + tracks['artist']

            daily_new_tracks = daily[~daily['concat'].isin(tracks['concat'])].drop('concat', axis=1)
            daily_repeated_tracks = daily.drop('concat', axis=1).merge(tracks.drop('concat', axis=1), on=['artist', 'track_name'], how='inner', suffixes=['_daily', '_old'])
            daily_out_tracks = tracks[(~tracks['concat'].isin(daily['concat'])) & (~tracks['current_rank'].isna())].drop('concat', axis=1)

            logging.info('Divided data.')

            daily_new_tracks = handle_new(daily_new_tracks, type='track')
            daily_repeated_tracks = handle_repeated(daily_repeated_tracks, type='track')
            daily_out_tracks = handle_out(daily_out_tracks, type='track', today=kwargs['today'])

            logging.info('Handled data.')

            daily_new_tracks['track_id'] = pd.NA

            cur.execute('SELECT DISTINCT artist_name, artist_id FROM "2024_domingo_nicolas_morelli_schema"."dim_artists"')
            artists = cur.fetch_dataframe().rename(columns={'artist_name': 'artist'})

            daily_new_tracks = daily_new_tracks.merge(artists, on='artist', how='inner')

            daily = pd.concat([daily_new_tracks, daily_repeated_tracks, daily_out_tracks]).drop_duplicates().reset_index(drop=True).drop('artist', axis=1)

            logging.info('Concatenated data.')

            cur.execute(f'SELECT MAX(track_id) FROM "2024_domingo_nicolas_morelli_schema"."{table_name}"')

            max_id = cur.fetch_dataframe().iloc[0, 0]

            for index, _ in daily[daily['track_id'].isna()].iterrows():
                daily.loc[index, 'track_id'] = max_id + 1
                max_id += 1

            logging.info('New ids generated.')

            cur.execute(f"""UPDATE "2024_domingo_nicolas_morelli_schema"."{table_name}"
                            SET last_known = 'No',
                                expiration_date = '{kwargs['today']}'
                            WHERE last_known = 'Yes'
                                AND (current_rank IS NOT NULL
                                    OR track_name || artist_id IN (SELECT name || artist_id FROM "2024_domingo_nicolas_morelli_schema"."staging_tracks_daily"
                                                                                            JOIN "2024_domingo_nicolas_morelli_schema"."dim_artists" ON artist = artist_name))
                        """)
            conn.commit()
            logging.info('Updated previous last known.')

    except redshift_connector.error.ProgrammingError:
        with conn.cursor() as cur:
            logging.info('Table not found. Creating.')
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
            conn.commit()
            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_tracks_daily"')
            daily = cur.fetch_dataframe()

            logging.info('Fetched daily data.')

            daily = handle_new(daily, type='track')
            daily = daily.drop_duplicates().reset_index(names='track_id')

            logging.info('Handled new data.')

            cur.execute('SELECT DISTINCT artist_name, artist_id FROM "2024_domingo_nicolas_morelli_schema"."dim_artists"')
            artists = cur.fetch_dataframe().rename(columns={'artist_name': 'artist'})

            logging.info('Fetched artist.')

        daily = daily.merge(artists, on='artist', how='inner').drop('artist', axis=1)

    logging.info('Data ready.')
    return daily


@from_redshift_to_redshift
def albums_dim(*args, **kwargs) -> pd.DataFrame:
    """Generates album dimension in Pandas dataframe

    :return: Album dimension data.
    :rtype: pd.DataFrame
    """
    conn = kwargs['conn']
    table_name = kwargs['table_name']

    try:
        with conn.cursor() as cur:
            cur.execute(f"""

                        SELECT tn.*, a.artist_name AS artist
                        FROM "2024_domingo_nicolas_morelli_schema"."{table_name}" tn
                        JOIN (SELECT DISTINCT artist_id, artist_name FROM "2024_domingo_nicolas_morelli_schema".dim_artists) a ON a.artist_id = tn.artist_id
                        WHERE tn.last_known = 'Yes'

                        """)
            albums = cur.fetch_dataframe()

            logging.info('Fetched last known data.')

            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_albums_daily"')
            daily = cur.fetch_dataframe()

            logging.info('Fetched daily data.')

            daily = daily.rename(columns={'name': 'album_name', 'stats_date': 'effective_date'})

            daily['concat'] = daily['album_name'] + daily['artist']
            albums['concat'] = albums['album_name'] + albums['artist']

            daily_new_albums = daily[~daily['concat'].isin(albums['concat'])].drop('concat', axis=1)
            daily_repeated_albums = daily.drop('concat', axis=1).merge(albums.drop('concat', axis=1), on=['album_name', 'artist'], how='inner', suffixes=['_daily', '_old'])
            daily_out_albums = albums[(~albums['concat'].isin(daily['concat']))].drop('concat', axis=1)

            logging.info('Divided data.')

            daily_new_albums = handle_new(daily_new_albums, type='album')
            daily_repeated_albums = handle_repeated(daily_repeated_albums, type='album')
            daily_out_albums = handle_out(daily_out_albums, type='album', today=kwargs['today'])

            logging.info('Handled data.')

            daily_new_albums['album_id'] = pd.NA

            cur.execute('SELECT DISTINCT artist_name, artist_id FROM "2024_domingo_nicolas_morelli_schema"."dim_artists"')
            artists = cur.fetch_dataframe().rename(columns={'artist_name': 'artist'})

            daily_new_albums = daily_new_albums.merge(artists, on='artist', how='inner')

            daily = pd.concat([daily_new_albums, daily_repeated_albums, daily_out_albums]).drop_duplicates().reset_index(drop=True).drop('artist', axis=1)

            logging.info('Concatenated data.')

            cur.execute(f'SELECT MAX(album_id) FROM "2024_domingo_nicolas_morelli_schema"."{table_name}"')

            max_id = cur.fetch_dataframe().iloc[0, 0]

            for index, _ in daily[daily['album_id'].isna()].iterrows():
                daily.loc[index, 'album_id'] = max_id + 1
                max_id += 1

            logging.info('New ids generated.')

            cur.execute(f"""UPDATE "2024_domingo_nicolas_morelli_schema"."{table_name}"
                            SET last_known = 'No',
                                expiration_date = '{kwargs['today']}'
                            WHERE last_known = 'Yes'
                                AND album_name || artist_id IN (SELECT name || artist_id FROM "2024_domingo_nicolas_morelli_schema"."staging_albums_daily"
                                                                                        JOIN "2024_domingo_nicolas_morelli_schema"."dim_artists" ON artist = artist_name)
                        """)
            conn.commit()

            logging.info('Updated previous last known.')

    except redshift_connector.error.ProgrammingError:
        with conn.cursor() as cur:
            logging.info('Table not found. Creating.')
            conn.commit()
            cur.execute(f"""
                            CREATE TABLE "2024_domingo_nicolas_morelli_schema"."{table_name}"
                            (
                              id INTEGER IDENTITY(1, 1),
                              album_id INTEGER,
                              artist_id INTEGER,
                              album_name VARCHAR,
                              playcount INTEGER,
                              new_plays INTEGER,
                              effective_date VARCHAR,
                              expiration_date VARCHAR,
                              last_known VARCHAR
                            )

                        """)
            conn.commit()
            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_albums_daily"')
            daily = cur.fetch_dataframe()

            logging.info('Fetched daily data.')

            daily = handle_new(daily, type='album')
            daily = daily.drop_duplicates().reset_index(names='album_id')

            logging.info('Handled new data.')

            cur.execute('SELECT DISTINCT artist_name, artist_id FROM "2024_domingo_nicolas_morelli_schema"."dim_artists"')
            artists = cur.fetch_dataframe().rename(columns={'artist_name': 'artist'})

            logging.info('Fetched artist.')

        daily = daily.merge(artists, on='artist', how='inner').drop('artist', axis=1)

    logging.info('Data ready.')
    return daily
