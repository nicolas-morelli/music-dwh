# Introduccion
Este proyecto tiene por fin realizar un ETL completo, utilizando Pandas, Airflow y una base de datos Redshift.

## API
Se utilizo la API provista por la pagina web last.fm, aplicacion que registra las canciones que reproducen sus usuarios a partir de otras como Spotify, Youtube Music, etc.

Las consultas se realizaron en base a tags que se les asigna a los artistas. En este caso se tomaron 12 generos populares de metal para obtener, a partir de los mismos, el top 50 de artistas de cada uno en base al dia con un GET por cada genero. De cada artista tambien se optiene el top 50 de sus canciones y sus albumes con dos GET, uno por concepto, por artista.

IMPORTANTE: No es posible obtener la historia pasada dada la estructura de la API, donde cada dia el GET otorga los datos nuevos y no hay metodos donde se pueda consultar fechas anteriores. El funcionamiento del proyecto depende del paso del tiempo para tener datos muy variados.

## Archivos
Los archivos principales del proyecto son:

### airflow/dags/dag.py
Este archivo contiene el dag de ejecucion en Airflow, con el detalle del proceso de ETL y el orden de ejecucion.

### airflow/dags/apifunctions/api.py
Este archivo contiene las funciones que realizan el staging de los datos, obtiendo los mismos de la API y realizando una transformacion inicial.

### airflow/dags/apifunctions/dims.py
Este archivo contiene las funciones de transformacion de dimensiones.

### airflow/dags/apifunctions/facts.py
Este archivo contiene las funciones de transformacion de tablas de hechos.

## Modelo de datos
### Tablas
#### Staging
A partir de la informacion obtenida de la API se generan las siguientes tablas que se renuevan diariamente:
- staging_artists_daily
- staging_tracks_daily
- staging_albums_daily

Ademas, se generan las siguientes tablas en caso de requerir informacion historica (las mismas no son utilizadas pero podrian ser de utilidad en casos de desastre):
- backup_staging_artists_daily
- backup_staging_tracks_daily
- backup_staging_albums_daily

#### Dimensiones
Utilizando las tablas de staging se producen las siguientes dimensiones:
- dim_artists
  - id: Primary key de la tabla
  - artist_id: ID del artista
  - artist_name: Nombre del artista
  - artist_tag: Tag/Genero al que pertenece
  - max_rank: Maximo ranking alcanzado en el tag
  - current_rank: Ranking actual, nulo en caso de no estar en el top
  - listeners: Oyentes
  - new_listeners: Oyentes incorporados desde la ultima vez visto
  - playcount: Reproducciones
  - new_plays: Reproducciones nuevas desde la ultima vez visto
  - consecutive_times_in_top_50: Cuantos dias se encontro en el ranking
  - effective_date: Dia de validez del registro
  - expiration_date: Vencimiento del registro
  - last_known: Si registro es el ultimo conocido
- dim_tracks
  - id: Primary key de la tabla
  - track_id: ID del track
  - artist_id: ID del artista creador del track, foreign key
  - track_name: Nombre del track
  - max_rank: Maximo ranking alcanzado en el top del artista
  - current_rank: Ranking actual, nulo en caso de no estar en el top
  - listeners: Oyentes
  - new_listeners: Oyentes incorporados desde la ultima vez visto
  - playcount: Reproducciones
  - new_plays: Reproducciones nuevas desde la ultima vez visto
  - effective_date: Dia de validez del registro
  - expiration_date: Vencimiento del registro
  - last_known: Si registro es el ultimo conocido
- dim_albums
  - id: Primary key de la tabla
  - album_id: ID del album
  - artist_id: ID del artista creador del album, foreign key
  - album_name: Nombre del album
  - playcount: Reproducciones
  - new_plays: Reproducciones nuevas desde la ultima vez visto
  - effective_date: Dia de validez del registro
  - expiration_date: Vencimiento del registro
  - last_known: Si registro es el ultimo conocido

Las mismas son SCD2, actualizandose en caso de que un artista del top 50 vuelva a entrar en el top diario.

#### Hechos
Utilizando las tablas de staging y dimensiones se producen las siguientes tablas de hechos:
- fact_tracks
- fact_albums

## Como ejecutar
### .env
Es necesario contar con un archivo .env dentro del directorio airflow la siguiente estructura:

REDSHIFT_HOST=Host de la base de datos Redshift
REDSHIFT_PORT=Puerto de la base de datos Redshift
REDSHIFT_DB=Base de datos Redshift
REDSHIFT_USER=Usuario de la base de datos
REDSHIFT_PW=Contraseña del usuario

LASTFM_KEY=Llave de la API de last.fm
LASTFM_SECRET=Secreto de la API de last.fm (no se utiliza en el proyecto de momento y puede ser obviada)

### Airflow
Se debe realizar compose del docker-compose provisto, el cual creara una instancia de Airflow en localhost::8080. Alli, con usuario y contraseña 'airflow' debera entrar y activar el DAG.

Esto permitira la ejecucion diaria del ETL, lo cual generara los datos con el paso del tiempo.

### Sphinx
En la carpeta docs ejecutar `sphinx-quickstart (debe instalar sphinx previamente).

Luego, en el archivo conf.py incluir 

```python 
import os
import sys

sys.path.insert(0, os.path.abspath('../airflow'))
```

Y agregue en extensions 'sphinx.ext.autodoc'.

En terminal ejecute los siguientes comandos

```
sphinx-apidoc -o .\music-dwh\docs .\music-dwh
make html
```

Deberia visualizar la documentacion correctamente.