import os
import yaml
import logging
import redshift_connector
import awswrangler as wr


def handle_new_artists(daily):
    daily = daily.rename(columns={'name': 'artist_name', 'rank': 'current_rank', 'stats_date': 'effective_date'})
    daily['max_rank'] = daily['current_rank']
    daily['new_listeners'] = daily['listeners']
    daily['new_plays'] = daily['playcount']
    daily['expiration_date'] = '9999-12-31'
    daily['current'] = 'Yes'
    daily['times_in_top_50'] = 1

    return daily


def handle_repeated_artists(daily):
    daily['new_plays'] = daily['playcount_daily'] - daily['playcount_artists'].fillna(0)
    daily['playcount'] = daily['playcount_daily']
    daily['new_listeners'] = daily['listeners_daily'] - daily['listeners_artists'].fillna(0)
    daily['listeners'] = daily['listeners_daily']
    daily['max_rank'] = daily[['current_rank_daily', 'max_rank']].max(axis=1)
    daily['current_rank'] = daily['current_rank_daily']
    daily['expiration_date'] = '9999-12-31'

    cols_to_drop = []
    for col in daily.columns:
        if '_daily' in col or '_artists' in col:
            cols_to_drop.append(col)
    cols_to_drop.append('id')

    daily = daily.drop(cols_to_drop, axis=1)

    return daily


def handle_out_artists():


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

        table_name = kwargs['table_name']
        wr.redshift.to_sql(df=df_api, con=conn, table=table_name, schema='2024_domingo_nicolas_morelli_schema', mode='append', index=False)
        logging.info(f'{table_name} loaded.')

        return

    return wrapper


# TODO: Cuando ya haya pensado logica, sumarle TOP track, TOP album y tag tal vez
@from_redshift_to_redshift
def artist_dim(*args, **kwargs):
    conn = kwargs['conn']

    with conn.cursor() as cur:
        try:
            # TODO: Deberia traer todas las instancias mas recientes de cada artista, current o no
            cur.execute(""" SELECT * FROM "2024_domingo_nicolas_morelli_schema"."dim_artists" WHERE current = 'Yes' """)
            artists = cur.fetch_dataframe()

            # TODO: Updatear registros con expiration y expirarlas, y setear current = 'No'

            # Traigo nuevo top 50
            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_daily_artists"')
            daily = cur.fetch_dataframe()

            daily = daily.rename(columns={'name': 'artist_name', 'rank': 'current_rank', 'stats_date': 'effective_date'})

            daily_new_artists = daily[~daily['artist_name'].isin(artists['artist_name'])]
            daily_repeated_artists = daily.merge(artists, on='artist_name', how='inner', suffixes=['_daily', '_artists'])
            daily_out_artists = artists[~artists['artist_name'].isin(daily['artist_name'])]

            daily_new_artists = handle_new_artists(daily_new_artists)
            daily_repeated_artists = handle_repeated_artists(daily_repeated_artists)
            daily_out_artists = handle_out_artists(daily_out_artists)

            daily = 

        except Exception:
            cur.execute("""
                            CREATE TABLE "2024_domingo_nicolas_morelli_schema"."dim_artists"
                            (
                              id INTEGER IDENTITY(1, 1),
                              artist_id INTEGER [NOT NULL],
                              artist_name VARCHAR(255) [NOT NULL],
                              max_rank INTEGER,
                              current_rank INTEGER,
                              listeners INTEGER,
                              new_listeners INTEGER,
                              playcount INTEGER,
                              new_plays INTEGER,
                              consecutive_times_in_top_50 INTEGER,
                              effective_date VARCHAR(255),
                              expiration_date VARCHAR(255),
                              current VARCHAR(255)
                            )

                        """)
            cur.execute('SELECT * FROM "2024_domingo_nicolas_morelli_schema"."staging_daily_artists"')

            daily = cur.fetch_dataframe()

            daily = handle_new_artists(daily)
            daily = daily.drop('tag', axis=1).drop_duplicates().reset_index(names='artist_id')

        return daily


def tag_dim(*args, **kwargs):
    # Tags
    # TODO: Pruebo consulta y sino creo la tabla
    # TODO: Me fijo los artistas, si hay alguno nuevo lo agrego y le creo un ID
    # TODO: Con SCD 2 actualizo los dias viejos y creo el nuevo con los datos del momento
    # Totalidad de escuchas, promedio de listeners del top 50, artista actual en el rank 1, cancion de algun artista del top 50 con mas escuchas
    pass
