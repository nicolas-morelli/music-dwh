import json
import requests
import pandas as pd
from itertools import chain
from datetime import datetime


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
    artist['rank'] = artists.loc[index, 'rank']
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
        tracklist.append(process_track(i, tracks, artists.loc[index, 'name']))

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
    track['rank'] = track['@attr']['rank']
    track['stats_date'] = datetime.now().strftime('%Y-%m-%d')  # TODO: No usar, usar el del context de Airflow
    track['artist'] = name
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
        albumlist.append(process_album(i, albums, artists.loc[index, 'name']))

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


def main():
    # Leo credenciales de Last.fm de JSON
    with open('creds.json', 'r') as creds:
        creds = json.load(creds)
        key = creds.get('key')

    tags = ['heavy+metal', 'thrash+metal', 'industrial+metal', 'progressive+metal', 'power+metal', 'death+metal', 'deathcore']
    alltagartists = pd.DataFrame()

    for tag in tags:
        tagartists = pd.DataFrame(requests.get(f'https://ws.audioscrobbler.com/2.0/?method=tag.getTopArtists&tag={tag}&api_key={key}&format=json').json()['topartists']['artist'])[['name', 'url', 'mbid']].reset_index(names='rank')
        tagartists['rank'] = tagartists['rank'] + 1
        tagartists['tag'] = tag
        alltagartists = pd.concat([alltagartists, tagartists])

    artistfact = []
    albumbsfact = []
    tracksfact = []

    for index, artist in alltagartists.iterrows():
        name = artist['name'].replace('&', '').replace(' ', '+')
        tag = artist['tag'].replace('+', ' ').title()

        artistfact.append(process_artist(tag, name, key, tagartists, index))
        albumbsfact.append(process_albums(name, key, tagartists, index))
        tracksfact.append(process_tracks(name, key, tagartists, index))

    artistfact = pd.DataFrame(artistfact)
    albumbsfact = list(chain(*albumbsfact))
    albumbsfact = pd.DataFrame(albumbsfact)
    tracksfact = list(chain(*tracksfact))
    tracksfact = pd.DataFrame(tracksfact)

    print(artistfact)

    # TODO: Si artista/tag existe en la DIM, reemplazar nombres por IDs
    # TODO: Si no existe, crear ID, agregar en DIM, reemplazar en FACT
    # TODO: Crear tablas


if __name__ == "__main__":
    main()
