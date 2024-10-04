# Introduccion
Este proyecto tiene por fin realizar un ETL completo, utilizando Pandas, Airflow y una base de datos Redshift.

## API
Se utilizo la API provista por la pagina web last.fm, aplicacion que registra las canciones que reproducen sus usuarios a partir de otras como Spotify, Youtube Music, etc.

Las consultas se realizaron en base a tags que se les asigna a los artistas. En este caso se tomaron 12 generos populares de metal para obtener, a partir de los mismos, el top 50 de artistas de cada uno en base al dia con un GET por cada genero. De cada artista tambien se optiene el top 50 de sus canciones y sus albumes con dos GET, uno por concepto, por artista.

IMPORTANTE: No es posible obtener la historia pasada dada la estructura de la API, donde cada dia el GET otorga los datos nuevos y no hay metodos donde se pueda consultar fechas anteriores. El funcionamiento del proyecto depende del paso del tiempo para tener datos muy variados.

## Modelo de datos
### Tablas
A partir de la informacion obtenida de la API se generan las siguientes tablas:

## Como ejecutar
### .env\/.cfg\/creds.yaml
Es necesario contar con un archivo YAML en esas carpetas con el nombre creds.yaml la siguiente estructura:

redshift:
  host: Host de la base de datos Redshift
  port: Puerto de la base de datos Redshift
  db: Base de datos Redshift
  user: Usuario de la base de datos
  password: Contraseña del usuarioo

lastfm:
  key: Llave de la API de last.fm
  secret: Secreto de la API de last.fm (no se utiliza en el proyecto de momento y puede ser obviada)

### Airflow
Se debe realizar compose del docker-compose provisto, el cual creara una instancia de Airflow en localhost::8080. Alli, con usuario y contraseña 'airflow' debera entrar y activar el DAG.