import json
import requests
import pandas as pd
from datetime import datetime


def main():
    # Leo credenciales de Last.fm de JSON
    with open('creds.json', 'r') as creds:
        creds = json.load(creds)
        key = creds.get('key')

    # Obtengo TOP 50 Argentina
    argartists = pd.DataFrame(requests.get(f'https://ws.audioscrobbler.com/2.0/?method=tag.getTopArtists&tag=argentina&api_key={key}&format=json').json()['topartists']['artist'])[['name', 'url', 'mbid']].reset_index(names='rank')
    argartists['rank'] = argartists['rank'] + 1

    artistfact = []
    for index, artist in argartists.iterrows():
        name = artist['name'].replace('&', '').replace(' ', '+')
        try:
            artist = requests.get(f'https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={name}&api_key={key}&format=json')
            artist = {col: artist.json()['artist'][col] for col in artist.json()['artist'] if col in ('name', 'stats')}

            artist['listeners'] = artist['stats']['listeners']
            artist['playcount'] = artist['stats']['playcount']
            artist['rank'] = argartists.loc[index, 'rank']
            artist['stats_date'] = datetime.now().strftime('%Y-%m-%d') 

            del artist['stats']

            artistfact.append(artist)
        except Exception:
            pass

    artistfact = pd.DataFrame(artistfact)

    # TODO: artistfact to redshift

    # TODO: Sacar top temas

    # TODO: Sacar top albums

    # TODO: Actualizar DIM con top tema, top album, top rank, nuevos valores de listeners y reproducciones
