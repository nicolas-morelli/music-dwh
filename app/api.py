import json
import requests
import pandas as pd
from itertools import chain
from datetime import datetime


def process_artist(name, key, argartists, index):
    try:
        artist = requests.get(f'https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={name}&api_key={key}&format=json')
        artist = {col: artist.json()['artist'][col] for col in artist.json()['artist'] if col in ('name', 'stats')}
        artist['listeners'] = artist['stats']['listeners']
        artist['playcount'] = artist['stats']['playcount']
        artist['rank'] = argartists.loc[index, 'rank']
        artist['stats_date'] = datetime.now().strftime('%Y-%m-%d')  # TODO: No usar, usar el del context de Airflow

        del artist['stats']

    except Exception:
        return None

    return artist


def process_tracks(name, key, argartists, index):
    tracks = requests.get(f'https://ws.audioscrobbler.com/2.0/?method=artist.gettoptracks&artist={name}&api_key={key}&format=json')
    tracklist = []

    for i in range(0, len(tracks.json()['toptracks']['track'])):
        tracklist.append(process_track(i, tracks, argartists.loc[index, 'name']))

    return tracklist


def process_track(i, tracks, name):
    try:
        track = {col: tracks.json()['toptracks']['track'][i][col] for col in tracks.json()['toptracks']['track'][i] if col in ('name', 'playcount', 'listeners', '@attr')}

        track['rank'] = track['@attr']['rank']
        track['stats_date'] = datetime.now().strftime('%Y-%m-%d')  # TODO: No usar, usar el del context de Airflow
        track['artist'] = name
        del track['@attr']

        return track

    except Exception:
        return None


def process_albums(name, key, argartists, index):
    albums = requests.get(f'https://ws.audioscrobbler.com/2.0/?method=artist.gettopalbums&artist={name}&api_key={key}&format=json')
    albumlist = []

    for i in range(0, len(albums.json()['topalbums']['album'])):
        albumlist.append(process_album(i, albums, argartists.loc[index, 'name']))

    return albumlist


def process_album(i, albums, name):
    try:
        album = {col: albums.json()['topalbums']['album'][i][col] for col in albums.json()['topalbums']['album'][i] if col in ('name', 'playcount')}

        album['stats_date'] = datetime.now().strftime('%Y-%m-%d')  # TODO: No usar, usar el del context de Airflow
        album['artist'] = name

        return album

    except Exception:
        return None


def main():
    # Leo credenciales de Last.fm de JSON
    with open('creds.json', 'r') as creds:
        creds = json.load(creds)
        key = creds.get('key')

    tag = 'heavy+metal'

    tagartists = pd.DataFrame(requests.get(f'https://ws.audioscrobbler.com/2.0/?method=tag.getTopArtists&tag={tag}&api_key={key}&format=json').json()['topartists']['artist'])[['name', 'url', 'mbid']].reset_index(names='rank')
    tagartists['rank'] = tagartists['rank'] + 1

    artistfact = []
    albumbsfact = []
    tracksfact = []

    for index, artist in tagartists.iterrows():
        name = artist['name'].replace('&', '').replace(' ', '+')

        artistfact.append(process_artist(name, key, tagartists, index))
        albumbsfact.append(process_albums(name, key, tagartists, index))
        tracksfact.append(process_tracks(name, key, tagartists, index))

    artistfact = pd.DataFrame(artistfact)
    albumbsfact = list(chain(*albumbsfact))
    albumbsfact = pd.DataFrame(albumbsfact)
    tracksfact = list(chain(*tracksfact))
    tracksfact = pd.DataFrame(tracksfact)

    print(artistfact)

    # TODO: Actualizar DIM con top tema, top album, top rank, nuevos valores de listeners y reproducciones

    # TODO: todo a redshift


if __name__ == "__main__":
    main()
