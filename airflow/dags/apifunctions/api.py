import os
import yaml
import logging
from itertools import chain
from datetime import datetime
import requests
import redshift_connector
import awswrangler as wr
import pandas as pd


""" AUX FUNCTIONS """


def process_artist(tag: str, name: str, key: str, artists: pd.DataFrame, index: int) -> pd.DataFrame:
    """From an artists name, gets its details from the API and turns it into a DataFrame

    :param tag: Tag representative of the artists according to Last.fm
    :type tag: str
    :param name: Artists name
    :type name: str
    :param key: Last.fm key
    :type key: str
    :param artists: Artists to process
    :type artists: pd.DataFrame
    :param index: Position of artist in artists to process
    :type index: int
    :return: Processed artist
    :rtype: pd.DataFrame
    """

    artist = requests.get(f'https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={name}&api_key={key}&format=json')
    artist = {col: artist.json()['artist'][col] for col in artist.json()['artist'] if col in ('name', 'stats')}
    artist['tag'] = tag
    artist['listeners'] = artist['stats']['listeners']
    artist['playcount'] = artist['stats']['playcount']
    artist['rank'] = artists.at[index, 'rank']
    artist['stats_date'] = datetime.now().strftime('%Y-%m-%d')  # TODO: No usar, usar el del context de Airflow
    del artist['stats']

    return artist


def process_tracks(name: str, key: str, artists: pd.DataFrame, index: int) -> pd.DataFrame:
    """From an artists name, get its top tracks from the API and turn them into a Dataframe

    :param name: Artists name
    :type name: str
    :param key: Last.fm key
    :type key: str
    :param artists: Artists to process
    :type artists: pd.DataFrame
    :param index: Position of artist in artists to process
    :type index: int
    :return: Processed tracks for an artist
    :rtype: pd.DataFrame
    """

    tracks = requests.get(f'https://ws.audioscrobbler.com/2.0/?method=artist.gettoptracks&artist={name}&api_key={key}&format=json')
    tracklist = []

    for i in range(0, len(tracks.json()['toptracks']['track'])):
        tracklist.append(process_track(i, tracks, artists.at[index, 'name']))

    return tracklist


def process_track(i: int, tracks: requests.Response, name: str) -> pd.DataFrame:
    """From a dictionary of tracks and a position, process a certain track into a Dataframe

    :param i: Index of track
    :type i: int
    :param tracks: Tracks of an artist
    :type tracks: requests.Response
    :param name: Artists name
    :type name: str
    :return: Processed track
    :rtype: pd.DataFrame
    """

    track = {col: tracks.json()['toptracks']['track'][i][col] for col in tracks.json()['toptracks']['track'][i] if col in ('name', 'playcount', 'listeners', '@attr')}
    track['rank'] = int(track['@attr']['rank'])
    track['stats_date'] = datetime.now().strftime('%Y-%m-%d')  # TODO: No usar, usar el del context de Airflow
    track['artist'] = name
    track['name'] = track['name'][:80] + ('...' if len(track['name']) > 80 else '')

    del track['@attr']

    return track


def process_albums(name: str, key: str, artists: pd.DataFrame, index: int) -> pd.DataFrame:
    """From an artists name, get its top albums from the API and turn them into a Dataframe

    :param name: Artists name
    :type name: str
    :param key: Last.fm key
    :type key: str
    :param artists: Artists to process
    :type artists: pd.DataFrame
    :param index: Position of artist in artists to process
    :type index: int
    :return: Processed albums for an artist
    :rtype: pd.DataFrame
    """

    albums = requests.get(f'https://ws.audioscrobbler.com/2.0/?method=artist.gettopalbums&artist={name}&api_key={key}&format=json')
    albumlist = []

    for i in range(0, len(albums.json()['topalbums']['album'])):
        albumlist.append(process_album(i, albums, artists.at[index, 'name']))

    return albumlist


def process_album(i: int, albums: requests.Response, name: str) -> pd.DataFrame:
    """From a dictionary of albums and a position, process a certain track into a Dataframe

    :param i: Index of album
    :type i: int
    :param albums: Album of an artist
    :type albums: requests.Response
    :param name: Artists name
    :type name: str
    :return: Processed album
    :rtype: pd.DataFrame
    """

    album = {col: albums.json()['topalbums']['album'][i][col] for col in albums.json()['topalbums']['album'][i] if col in ('name', 'playcount')}
    album['stats_date'] = datetime.now().strftime('%Y-%m-%d')  # TODO: No usar, usar el del context de Airflow
    album['artist'] = name

    return album


def load_df_and_to_redshift(func):
    def wrapper(*args, **kwargs):
        pathcreds = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env/.cfg', 'creds.yaml')

        with open(pathcreds, 'r') as creds:
            creds = yaml.safe_load(creds)
            host = creds['redshift']['host']
            port = creds['redshift']['port']
            db = creds['redshift']['db']
            user = creds['redshift']['user']
            password = creds['redshift']['password']
            key = creds['lastfm']['key']
            logging.info('Credentials read.')

        alltagartists = pd.read_parquet(kwargs['ti'].xcom_pull(task_ids='extract_artists'))

        kwargs['key'] = key
        kwargs['alltagartists'] = alltagartists

        table_name = kwargs['table_name']

        df_api = func(*args, **kwargs)

        conn = redshift_connector.connect(database=db, user=user, password=password, host=host, port=port)
        wr.redshift.to_sql(df=df_api, con=conn, table=table_name, schema='2024_domingo_nicolas_morelli_schema', mode='overwrite', overwrite_method='drop', index=False)
        logging.info(f'{table_name} loaded.')
        return

    return wrapper


""" DAG FUNCTIONS """


def extract_artists() -> str:
    pathcreds = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env/.cfg', 'creds.yaml')

    with open(pathcreds, 'r') as creds:
        creds = yaml.safe_load(creds)
        key = creds['lastfm']['key']
        logging.info('Credentials read.')

    tags = ['heavy+metal',
            'thrash+metal',
            'nu+metal',
            'black+metal',
            'doom+metal',
            'industrial+metal',
            'progressive+metal',
            'power+metal',
            'symphonic+metal',
            'folk+metal',
            'death+metal',
            'deathcore']
    alltagartists = pd.DataFrame()

    for tag in tags:
        tagartists = pd.DataFrame(requests.get(f'https://ws.audioscrobbler.com/2.0/?method=tag.getTopArtists&tag={tag}&api_key={key}&format=json').json()['topartists']['artist'])[['name', 'url', 'mbid']].reset_index(names='rank')
        tagartists['rank'] = tagartists['rank'] + 1
        tagartists['tag'] = tag
        alltagartists = pd.concat([alltagartists, tagartists])
    alltagartists = alltagartists.reset_index(drop=True)

    artist_path = os.path.join(os.getcwd(), datetime.now().strftime('%Y-%m-%d') + '-ARTISTS.parquet')  # TODO: No usar, usar el del context de Airflow

    alltagartists.to_parquet(artist_path)

    return artist_path


@load_df_and_to_redshift
def etl_artist_data(**kwargs):
    alltagartists = kwargs['alltagartists']
    key = kwargs['key']

    artistdaily = []
    for index, artist in alltagartists.iterrows():
        name = artist['name'].replace('&', '').replace(' ', '+')
        tag = artist['tag'].replace('+', ' ').title()

        artistdaily.append(process_artist(tag, name, key, alltagartists, index))
        logging.info(f'{round(100 * len(artistdaily) / alltagartists.shape[0], 1)}% read. {name} loaded.')
    logging.info('Data ready.')

    return pd.DataFrame(artistdaily)


@load_df_and_to_redshift
def etl_track_data(**kwargs):
    alltagartists = kwargs['alltagartists']
    key = kwargs['key']

    tracksdaily = []
    for index, artist in alltagartists.iterrows():
        name = artist['name'].replace('&', '').replace(' ', '+')
        tag = artist['tag'].replace('+', ' ').title()

        tracksdaily.append(process_artist(tag, name, key, alltagartists, index))
        logging.info(f'{round(100 * len(tracksdaily) / alltagartists.shape[0], 1)}% read. {name} loaded.')
    logging.info('Data ready.')

    return pd.DataFrame(list(chain(*tracksdaily)))


@load_df_and_to_redshift
def etl_album_data(**kwargs):
    alltagartists = kwargs['alltagartists']
    key = kwargs['key']

    albumbsdaily = []
    for index, artist in alltagartists.iterrows():
        name = artist['name'].replace('&', '').replace(' ', '+')
        tag = artist['tag'].replace('+', ' ').title()

        albumbsdaily.append(process_artist(tag, name, key, alltagartists, index))
        logging.info(f'{round(100 * len(albumbsdaily) / alltagartists.shape[0], 1)}% read. {name} loaded.')
    logging.info('Data ready.')

    return pd.DataFrame(list(chain(*albumbsdaily)))
