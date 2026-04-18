"""
Script guia para generar dump.sql a partir de los archivos JSON del dataset.

Archivos JSON disponibles:
  - anime_full.json       -> datos de cada anime (info, generos, estudios, etc.)
  - anime_characters.json -> personajes y actores de voz por anime
  - anime_staff.json      -> staff (directores, productores, etc.) por anime

Los tres archivos son listas paralelas: el elemento i de cada archivo
corresponde al mismo anime.
"""
import json
from typing import Any


# =============================================================================
# Utilidades
# =============================================================================

def load_json(path: str) -> list[dict]:
    """Lee un archivo JSON y retorna su contenido como lista de diccionarios."""
    with open(path) as f:
        return json.load(f)


def esc(value: str | None) -> str:
    """Escapa un string para SQL. Retorna NULL si es None."""
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def val(v: Any) -> str:
    """Formatea cualquier valor Python como literal SQL."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return esc(str(v))


# =============================================================================
# Carga de datos
# =============================================================================

def load_all_data() -> tuple[list[dict], list[dict], list[dict]]:
    """Carga los tres archivos JSON y muestra un resumen para verificar."""
    animes = load_json("anime_full.json")
    characters = load_json("anime_characters.json")
    staff = load_json("anime_staff.json")

    print(f"Animes cargados: {len(animes)}")
    print(f"Entries de characters: {len(characters)}")
    print(f"Entries de staff: {len(staff)}")

    # Mostrar estructura del primer anime para entender el JSON
    first = animes[0]["data"]
    print(f"\n--- Ejemplo: primer anime ---")
    print(f"  mal_id: {first['mal_id']}")
    print(f"  title: {first['title']}")
    print(f"  episodes: {first['episodes']}")
    print(f"  score: {first['score']}")
    print(f"  season: {first.get('season')}")
    print(f"  rating: {first.get('rating')}")
    print(f"  type: {first.get('type')}")
    print(f"  source: {first.get('source')}")
    print(f"  genres: {first.get('genres', [])}")
    print(f"  studios: {first.get('studios', [])}")
    print(f"  producers: {first.get('producers', [])}")
    print(f"  themes: {first.get('themes', [])}")
    print(f"  demographics: {first.get('demographics', [])}")
    print(f"  titles: {first.get('titles', [])[:3]}...")
    print(f"  streaming: {first.get('streaming', [])[:2]}...")
    print(f"  relations: {len(first.get('relations', []))} relaciones")

    # Mostrar estructura del primer personaje
    first_char = characters[0]["data"][0]
    print(f"\n--- Ejemplo: primer personaje (anime {first['mal_id']}) ---")
    print(f"  character: {first_char['character']['mal_id']} - {first_char['character']['name']}")
    print(f"  role: {first_char['role']}")
    print(f"  favorites: {first_char['favorites']}")
    print(f"  voice_actors: {len(first_char['voice_actors'])} actores")
    if first_char["voice_actors"]:
        va = first_char["voice_actors"][0]
        print(f"    primer actor: {va['person']['mal_id']} - {va['person']['name']} ({va['language']})")

    # Mostrar estructura del primer staff
    first_staff = staff[0]["data"][0]
    print(f"\n--- Ejemplo: primer staff (anime {first_staff.get('anime_id')}) ---")
    print(f"  person: {first_staff['person']['mal_id']} - {first_staff['person']['name']}")
    print(f"  positions: {first_staff['positions']}")

    return animes, characters, staff

# =============================================================================
# EJEMPLO COMPLETO: tabla genre + anime_genre
# =============================================================================
# Tipos de columnas:
#   genre: genre_id INTEGER PK, type TEXT, name TEXT, url TEXT
#   anime_genre: anime_id INTEGER FK(anime), genre_id INTEGER FK(genre), PK(anime_id, genre_id)

def extract_genres(animes: list[dict]) -> list[dict]:
    """Extrae generos unicos de todos los animes."""
    genres: dict[int, dict] = {}
    for anime in animes:
        for g in anime["data"].get("genres", []):
            gid = g["mal_id"]
            if gid not in genres:
                genres[gid] = {
                    "genre_id": gid,
                    "type": g["type"],
                    "name": g["name"],
                    "url": g["url"],
                }
    return list(genres.values())


def extract_anime_genre(animes: list[dict]) -> list[tuple[int, int]]:
    """Extrae pares (anime_id, genre_id) para la tabla junction."""
    pairs: set[tuple[int, int]] = set()
    for anime in animes:
        aid = anime["data"]["mal_id"]
        for g in anime["data"].get("genres", []):
            pairs.add((aid, g["mal_id"]))
    return sorted(pairs)


def generate_genre_sql(genres: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS genre CASCADE;",
        "",
        "CREATE TABLE genre (",
        "    genre_id INTEGER PRIMARY KEY,",
        "    type TEXT,",
        "    name TEXT,",
        "    url TEXT",
        ");",
        "",
    ]
    for g in genres:
        lines.append(
            f"INSERT INTO genre (genre_id, type, name, url) "
            f"VALUES ({g['genre_id']}, {esc(g['type'])}, "
            f"{esc(g['name'])}, {esc(g['url'])});"
        )
    return "\n".join(lines)


def generate_anime_genre_sql(pairs: list[tuple[int, int]]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime_genre CASCADE;",
        "",
        "CREATE TABLE anime_genre (",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    genre_id INTEGER REFERENCES genre(genre_id),",
        "    PRIMARY KEY (anime_id, genre_id)",
        ");",
        "",
    ]
    for aid, gid in pairs:
        lines.append(
            f"INSERT INTO anime_genre (anime_id, genre_id) VALUES ({aid}, {gid});"
        )
    return "\n".join(lines)


# =============================================================================
# TODO: Tablas SERIAL (ID auto-generado)
# =============================================================================
# Estas tablas tienen un ID que PostgreSQL genera solo (SERIAL).
# Solo se inserta el nombre. Hay que mantener un mapeo nombre -> id
# para poder usarlo como FK en la tabla anime.
#
# Tipos de columnas:
#   season:    season_id SERIAL PK, name TEXT UNIQUE
#   rating:    rating_id SERIAL PK, name TEXT UNIQUE
#   type:      type_id SERIAL PK, name TEXT UNIQUE
#   source:    source_id SERIAL PK, name TEXT UNIQUE
#
# Fuente JSON: campos directos del anime, ej: anime["data"]["season"] -> "fall"

# TODO: implementar extraccion y generacion SQL para season, rating, type, source
def extract_season(animes: list[dict]) -> dict[str, dict]:
    """Extrae las temporadas """
    seasons = {}
    i = 1
    for anime in animes:
        season = anime["data"].get("season")

        if season not in seasons and season is not None:
            seasons[season] = {
                "season_id": i,
                "name": season
            }
            i+= 1
    return seasons

def generate_season_sql(seasons: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS season CASCADE;",
        "",
        "CREATE TABLE season (",
        "    season_id SERIAL PRIMARY KEY,",
        "    name TEXT UNIQUE",
        ");",
        "",
    ]
    for s in seasons:
        lines.append(
            f"INSERT INTO season (name)"
            f"VALUES ({esc('name')});"
        )
    return "\n".join(lines)

def extract_rating(animes: list[dict]) -> dict[str, dict]:
    """Extrae el rating de todos los animes."""
    ratings = {}
    i = 1
    for anime in animes:
        rating = anime["data"].get("rating")

        if rating not in ratings:
            ratings[rating] = {
                "rating_id": i,
                "name": rating
            }
            i+= 1
    return ratings

def generate_rating_sql(ratings: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS rating CASCADE;",
        "",
        "CREATE TABLE rating (",
        "    rating_id SERIAL PRIMARY KEY,",
        "    name TEXT UNIQUE",
        ");",
        "",
    ]
    for r in ratings:
        lines.append(
            f"INSERT INTO rating (name) ;"
            f"VALUES ({esc('name')});"
        )
    return "\n".join(lines)

def extract_type(animes: list[dict]) -> dict[str, dict]:
    """Extrae el tipo de todos los animes."""
    types = {}
    i = 1
    for anime in animes:
        type = anime["data"].get("type")

        if type not in types:
            types[type] = {
                "type_id": i,
                "name": type
            }
            i+= 1
    return types

def generate_type_sql(types: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS type CASCADE;",
        "",
        "CREATE TABLE type (",
        "    type_id SERIAL PRIMARY KEY,",
        "    name TEXT UNIQUE,",
        ");",
        "",
    ]
    for t in types:
        lines.append(
            f"INSERT INTO rating (name) ;"
            f"VALUES ({esc('name')});"
        )
    return "\n".join(lines)

def extract_source(animes: list[dict]) -> dict[str, dict]:
    """Extrae el source de todos los animes."""
    sources = {}
    i = 1
    for anime in animes:
        source = anime["data"].get("source")

        if source not in sources:
            sources[source] = {
                "source_id": i,
                "name": source
            }
            i+= 1
    return sources

def generate_source_sql(sources: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS source CASCADE;",
        "",
        "CREATE TABLE source (",
        "    source_id SERIAL PRIMARY KEY,",
        "    name TEXT UNIQUE",
        ");",
        "",
    ]
    for s in sources:
        lines.append(
            f"INSERT INTO source (name) ;"
            f"VALUES ({esc('name')});"
        )
    return "\n".join(lines)

# =============================================================================
# TODO: Tabla anime (tabla principal)
# =============================================================================
# Tipos de columnas:
#   anime_id INTEGER PK, url TEXT, image_url TEXT, small_image_url TEXT,
#   large_image_url TEXT, trailer_youtube_id TEXT, trailer_url TEXT,
#   trailer_embed_url TEXT, approved BOOLEAN, title_default TEXT,
#   title_japanese TEXT, title_english TEXT, episodes INTEGER, status TEXT,
#   airing BOOLEAN, aired_from DATE, aired_to DATE, duration TEXT,
#   score DECIMAL, scored_by INTEGER, rank INTEGER, popularity INTEGER,
#   members INTEGER, favorites INTEGER, synopsis TEXT, background TEXT,
#   year INTEGER, broadcast_day TEXT, broadcast_time TEXT,
#   broadcast_timezone TEXT, broadcast_string TEXT,
#   season_id INTEGER FK(season), rating_id INTEGER FK(rating),
#   type_id INTEGER FK(type), source_id INTEGER FK(source)
#
# Fuente JSON: anime["data"] contiene la mayoria de campos directamente.
#   Imagenes en anime["data"]["images"]["jpg"]
#   Trailer en anime["data"]["trailer"]
#   Fechas en anime["data"]["aired"]["from"] y ["to"] (formato ISO, usar [:10])
#   Broadcast en anime["data"]["broadcast"]
#   season_id, rating_id, type_id, source_id son FK -> usar mapeos SERIAL

# TODO: implementar extraccion y generacion SQL para anime
def extract_anime(animes: list[dict], seasons, ratings, types, sources) -> list[dict]:
    animes_list = []

    for anime in animes:
        data = anime.get("data", {})
        anime_id = data.get("mal_id")

        season = data.get("season")
        rating = data.get("rating")
        type_ = data.get("type")
        source = data.get("source")
        aired = data.get("aired", {})
        trailer = data.get("trailer", {})
        broadcast = data.get("broadcast", {})

        animes_list.append({
            "anime_id": anime_id,
            "url": data.get("url"),
            "image_url": data.get("images", {}).get("jpg", {}).get("image_url"),
            "small_image_url": data.get("images", {}).get("jpg", {}).get("small_image_url"),
            "large_image_url": data.get("images", {}).get("jpg", {}).get("large_image_url"),
            "trailer_youtube_id": trailer.get("youtube_id"),
            "trailer_url": trailer.get("url"),
            "trailer_embed_url": trailer.get("embed_url"),
            "approved": data.get("approved"),
            "title_default": data.get("title"),
            "title_japanese": data.get("title_japanese"),
            "title_english": data.get("title_english"),
            "episodes": data.get("episodes"),
            "status": data.get("status"),
            "airing": data.get("airing"),
            "aired_from": aired.get("from")[:10] if aired.get("from") else None,
            "aired_to": aired.get("to")[:10] if aired.get("to") else None,
            "duration": data.get("duration"),
            "score": data.get("score"),
            "scored_by": data.get("scored_by"),
            "rank": data.get("rank"),
            "popularity": data.get("popularity"),
            "members": data.get("members"),
            "favorites": data.get("favorites"),
            "synopsis": data.get("synopsis"),
            "background": data.get("background"),
            "year": data.get("year"),
            "broadcast_day": broadcast.get("day"),
            "broadcast_time": broadcast.get("time"),
            "broadcast_timezone": broadcast.get("timezone"),
            "broadcast_string": broadcast.get("string"),
            "season_id": seasons.get(season, {}).get("season_id"),
            "rating_id": ratings.get(rating, {}).get("rating_id"),
            "type_id": types.get(type_, {}).get("type_id"),
            "source_id": sources.get(source, {}).get("source_id")
            })

    return animes_list

def generate_anime_sql(animes_list: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime CASCADE;",
        "",
        "CREATE TABLE anime (",
        "    anime_id INTEGER PRIMARY KEY,",
        "    url TEXT,",
        "    image_url TEXT,",
        "    small_image_url TEXT,",
        "    large_image_url TEXT,",
        "    trailer_youtube_id TEXT,", 
        "    trailer_url TEXT," ,
        "    trailer_embed_url TEXT,",
        "    approved BOOLEAN,", 
        "    title_default TEXT,",
        "    title_japanese TEXT,",
        "    title_english TEXT,",
        "    episodes INTEGER,",  
        "    status TEXT,",
        "    airing BOOLEAN,",
        "    aired_from DATE,",
        "    aired_to DATE,",
        "    duration TEXT,",
        "    score DECIMAL,", 
        "    scored_by INTEGER,",
        "    rank INTEGER,",
        "    popularity INTEGER,",
        "    members INTEGER,",
        "    favorites INTEGER,", 
        "    synopsis TEXT,", 
        "    background TEXT,",
        "    year INTEGER,", 
        "    broadcast_day TEXT,", 
        "    broadcast_time TEXT,",
        "    broadcast_timezone TEXT,",
        "    broadcast_string TEXT,",
        "    season_id INTEGER REFERENCES season(season_id),",
        "    rating_id INTEGER REFERENCES rating(rating_id),",
        "    type_id INTEGER REFERENCES type(type_id),",
        "    source_id INTEGER REFERENCES source(source_id)",
        ");",
        "",
    ]
    for a in animes_list:
        lines.append(
            f"INSERT INTO anime (anime_id, url, image_url, small_image_url, large_image_url, trailer_youtube, trailer_url, trailer_embed_url, approved, title_default, title_japanese, title_english, episodes, status, airing, aired_from, aired_to, duration, score, scored_by, rank, popularity, members, favorites, synopsis, background, year, broadcast_day, broadcast_time, broadcast_timezone, broadcast_string, season_id, rating_id, type_id, source_id)"
            f" VALUES ({val(a['anime_id'])}, {val(a['url'])}, {val(a['image_url'])}, {val(a['small_image_url'])}, {val(a['large_image_url'])}, {val(a['trailer_youtube_id'])}, {val(a['trailer_url'])}, {val(a['trailer_embed_url'])}, {val(a['approved'])}, {val(a['title_default'])}, {val(a['title_japanese'])}, {val(a['title_english'])}, {val(a['episodes'])}, {val(a['status'])}, {val(a['airing'])}, {val(a['aired_from'])}, {val(a['aired_to'])}, {val(a['duration'])}, {val(a['score'])}, {val(a['scored_by'])}, {val(a['rank'])}, {val(a['popularity'])}, {val(a['members'])}, {val(a['favorites'])}, {val(a['synopsis'])}, {val(a['background'])}, {val(a['year'])}, {val(a['broadcast_day'])}, {val(a['broadcast_time'])}, {val(a['broadcast_timezone'])}, {val(a['broadcast_string'])}, {val(a['season_id'])}, {val(a['rating_id'])}, {val(a['type_id'])}, {val(a['source_id'])});"
        )
    return "\n".join(lines)

# =============================================================================
# TODO: Tabla title
# =============================================================================
# Tipos de columnas:
#   title_id SERIAL PK, anime_id INTEGER FK(anime), type TEXT, title TEXT
#
# Fuente JSON: anime["data"]["titles"] -> [{"type": "...", "title": "..."}]

# TODO: implementar extraccion y generacion SQL para title
def extract_titles(animes: list[dict]) -> list[dict]:
    """Extrae titulos de todos los animes."""
    titles = []
    i = 1

    for anime in animes:
        data = anime.get("data", {}).get("titles", [])

        for d in data:
            titles.append({
                "title_id": i,
                "anime_id": anime.get("data", {}).get("mal_id"),
                "type": d.get("type"),
                "title": d.get("title")
            })
            i += 1

    return titles

def generate_title_sql(titles: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS title CASCADE;",
        "",
        "CREATE TABLE title (",
        "    title_id SERIAL PRIMARY KEY,",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    type TEXT,",
        "    title TEXT",
        ");",
        "",
    ]
    for t in titles:
        lines.append(
            f"INSERT INTO title (anime_id, type, title) "
            f"VALUES ({val(t['anime_id'])}, {val(t['type'])}, {val(t['title'])});"
        )

    return "\n".join(lines)

# =============================================================================
# TODO: Tablas de entidad con ID natural (mal_id del JSON)
# Misma estructura que el ejemplo de genre: recorrer, extraer unicos, generar SQL.
# =============================================================================

# Tipos de columnas:
#   producer:    producer_id INTEGER PK, name TEXT, url TEXT
#   licensor:    licensor_id INTEGER PK, name TEXT, url TEXT
#   studio:      studio_id INTEGER PK, name TEXT, url TEXT
#   explicit_genre: explicit_genre_id INTEGER PK, type TEXT, name TEXT, url TEXT
#   theme:       theme_id INTEGER PK, type TEXT, name TEXT, url TEXT
#   demographic: demographic_id INTEGER PK, type TEXT, name TEXT, url TEXT
#
# Fuente JSON (todas en anime["data"]):
#   "producers"       -> [{"mal_id", "name", "url"}]
#   "licensors"       -> [{"mal_id", "name", "url"}]
#   "studios"         -> [{"mal_id", "name", "url"}]
#   "explicit_genres" -> [{"mal_id", "type", "name", "url"}]
#   "themes"          -> [{"mal_id", "type", "name", "url"}]
#   "demographics"    -> [{"mal_id", "type", "name", "url"}]

# TODO: implementar extraccion y generacion SQL para cada una
def extract_producer(animes: list[dict]) -> list[dict]:
    """Extrae productores unicos de todos los animes."""
    producers= {}
    for anime in animes:
        data = anime.get("data", {}).get("producers", [])

        for d in data:
            pid = d.get("mal_id")

            if pid not in producers:
                producers[pid] = {
                    "producer_id": pid,
                    "name": d.get("name"),
                    "url": d.get("url"),
                }

    return list(producers.values())

def generate_producers_sql(producers: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS producer CASCADE;",
        "",
        "CREATE TABLE producer (",
        "    producer_id INTEGER PRIMARY KEY,",
        "    name TEXT,",
        "    url TEXT",
        ");",
        "",
    ]
    for p in producers:
        lines.append(
            f"INSERT INTO producers (producer_id, name, url) "
            f"VALUES ({val(p['producer_id'])}, {val(p['name'])}, {val(p['url'])});"
        )

    return "\n".join(lines)

def extract_licensor(animes: list[dict]) -> list[dict]:
    """Extrae licenciatarios unicos de todos los animes."""
    licensors = {}

    for anime in animes:
        data = anime.get("data", {}).get("licensors", [])

        for d in data:
            lid = d.get("mal_id")
    
            if lid not in licensors:
                licensors[lid] = {
                    "licensor_id": lid,
                    "name": d.get("name"),
                    "url": d.get("url"),
                }

    return list(licensors.values())

def generate_licensors_sql(licensors: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS licensor CASCADE;",
        "",
        "CREATE TABLE licensor (",
        "    licensor_id INTEGER PRIMARY KEY,",
        "    name TEXT,",
        "    url TEXT",
        ");",
        "",
    ]
    for l in licensors:
        lines.append(
            f"INSERT INTO licensor (licensor_id, name, url) "
            f"VALUES ({val(l['licensor_id'])}, {val(l['name'])}, {val(l['url'])});"
        )

    return "\n".join(lines)

def extract_studio(animes: list[dict]) -> list[dict]:
    """Extrae estudios unicos de todos los animes."""
    studios = {}

    for anime in animes:
        data = anime.get("data", {}).get("studios", [])

        for d in data:
            sid = d.get("mal_id")
    
            if sid not in studios:
                studios[sid] = {
                    "studio_id": sid,
                    "name": d.get("name"),
                    "url": d.get("url"),
                }

    return list(studios.values())

def generate_studios_sql(studios: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS studio CASCADE;",
        "",
        "CREATE TABLE studio (",
        "    studio_id INTEGER PRIMARY KEY,",
        "    name TEXT,",
        "    url TEXT",
        ");",
        "",
    ]
    for s in studios:
        lines.append(
            f"INSERT INTO studio (studio_id, name, url) "
            f"VALUES ({val(s['studio_id'])}, {val(s['name'])}, {val(s['url'])});"
        )

    return "\n".join(lines)

def extract_explicit_genres(animes: list[dict]) -> list[dict]:
    """Extrae generos explicitos unicos de todos los animes."""
    explicit_genres = {}

    for anime in animes:

        data = anime.get("data", {}).get("explicit_genres", [])

        for d in data:
            egid = d.get("mal_id")
    
            if egid not in explicit_genres:
                explicit_genres[egid] = {
                    "explicit_genre_id": egid,
                    "type": d.get("type"),
                    "name": d.get("name"),
                    "url": d.get("url"),
                }

    return list(explicit_genres.values())

def generate_explicit_genres_sql(explicit_genres: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS explicit_genre CASCADE;",
        "",
        "CREATE TABLE explicit_genre (",
        "    explicit_genre_id INTEGER PRIMARY KEY,",
        "    type TEXT,",
        "    name TEXT,",
        "    url TEXT",
        ");",
        "",
    ]
    for e in explicit_genres:
        lines.append(
            f"INSERT INTO explicit_genres (explicit_genre_id, type, name, url) "
            f"VALUES ({val(e['explicit_genre_id'])}, {val(e['type'])}, {val(e['name'])}, {val(e['url'])});"
        )

    return "\n".join(lines)

def extract_themes(animes: list[dict]) -> list[dict]:
    """Extrae temas  unicos de todos los animes."""
    themes = {}

    for anime in animes:
        data = anime.get("data", {}).get("themes", [])

        for d in data:
            tid = d.get("mal_id")
    
            if tid not in themes:
                themes[tid] = {
                    "theme_id": tid,
                    "type": d.get("type"),
                    "name": d.get("name"),
                    "url": d.get("url"),
                }

    return list(themes.values())

def generate_themes_sql(themes: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS theme CASCADE;",
        "",
        "CREATE TABLE theme (",
        "    theme_id INTEGER PRIMARY KEY,",
        "    type TEXT,",
        "    name TEXT,",
        "    url TEXT",
        ");",
        "",
    ]
    for t in themes:
        lines.append(
            f"INSERT INTO theme (theme_id, type, name, url) "
            f"VALUES ({val(t['theme_id'])}, {val(t['type'])}, {val(t['name'])}, {val(t['url'])});"
        )

    return "\n".join(lines)

def extract_demographics(animes: list[dict]) -> list[dict]:
    """Extrae demografías explicitos unicos de todos los animes."""
    demographics = {}

    for anime in animes:
        data = anime.get("data", {}).get("demographics", [])

        for d in data:
            did = d.get("mal_id")
    
            if did not in demographics:
                demographics[did] = {
                    "demographic_id": did,
                    "type": d.get("type"),
                    "name": d.get("name"),
                    "url": d.get("url"),
                }

    return list(demographics.values())

def generate_demographics_sql(demographics: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS demographic CASCADE;",
        "",
        "CREATE TABLE demographic (",
        "    demographic_id INTEGER PRIMARY KEY,",
        "    type TEXT,",
        "    name TEXT,",
        "    url TEXT",
        ");",
        "",
    ]
    for d in demographics:
        lines.append(
            f"INSERT INTO demographic (demographic_id, type, name, url) "
            f"VALUES ({val(d['demographic_id'])}, {val(d['type'])}, {val(d['name'])}, {val(d['url'])});"
        )

    return "\n".join(lines)

# =============================================================================
# TODO: Tablas junction (relacion N:M entre anime y las entidades anteriores)
# =============================================================================
# Misma estructura que anime_genre: pares (anime_id, entidad_id).
#
# Tipos de columnas:
#   anime_producer:       anime_id INTEGER FK(anime), producer_id INTEGER FK(producer), PK(anime_id, producer_id)
#   anime_licensor:       anime_id INTEGER FK(anime), licensor_id INTEGER FK(licensor), PK(anime_id, licensor_id)
#   anime_studio:         anime_id INTEGER FK(anime), studio_id INTEGER FK(studio), PK(anime_id, studio_id)
#   anime_explicit_genre: anime_id INTEGER FK(anime), explicit_genre_id INTEGER FK(explicit_genre), PK(anime_id, explicit_genre_id)
#   anime_theme:          anime_id INTEGER FK(anime), theme_id INTEGER FK(theme), PK(anime_id, theme_id)
#   anime_demographic:    anime_id INTEGER FK(anime), demographic_id INTEGER FK(demographic), PK(anime_id, demographic_id)

# TODO: implementar extraccion y generacion SQL para cada junction
def anime_producer(animes: list[dict]) -> list[tuple[int, int]]:
    """Extrae pares (anime_id, producer_id) para la tabla junction."""
    pairs: set[tuple[int, int]] = set()
    for anime in animes:
        aid = anime["data"]["mal_id"]
        for p in anime["data"].get("producers", []):
            pairs.add((aid, p["mal_id"]))
    return sorted(pairs)

def generate_anime_producer_sql(pairs: list[tuple[int, int]]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime_producer CASCADE;",
        "",
        "CREATE TABLE anime_producer (",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    producer_id INTEGER REFERENCES producer(producer_id),",
        "    PRIMARY KEY (anime_id, producer_id)",
        ");",
        "",
    ]
    for aid, pid in pairs:
        lines.append(
            f"INSERT INTO anime_producer (anime_id, producer_id) VALUES ({aid}, {pid});"
        )
    return "\n".join(lines)

def anime_licensor(animes: list[dict]) -> list[tuple[int, int]]:
    """Extrae pares (anime_id, licensor_id) para la tabla junction."""
    pairs: set[tuple[int, int]] = set()
    for anime in animes:
        aid = anime["data"]["mal_id"]
        for l in anime["data"].get("licensors", []):
            pairs.add((aid, l["mal_id"]))
    return sorted(pairs)

def generate_anime_licensor_sql(pairs: list[tuple[int, int]]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime_licensor CASCADE;",
        "",
        "CREATE TABLE anime_licensor (",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    licensor_id INTEGER REFERENCES licensor(licensor_id),",
        "    PRIMARY KEY (anime_id, licensor_id)",
        ");",
        "",
    ]
    for aid, lid in pairs:
        lines.append(
            f"INSERT INTO anime_licensor (anime_id, licensor_id) VALUES ({aid}, {lid});"
        )
    return "\n".join(lines)

def anime_studio(animes: list[dict]) -> list[tuple[int, int]]:
    """Extrae pares (anime_id, studio_id) para la tabla junction."""
    pairs: set[tuple[int, int]] = set()
    for anime in animes:
        aid = anime["data"]["mal_id"]
        for s in anime["data"].get("studios", []):
            pairs.add((aid, s["mal_id"]))
    return sorted(pairs)

def generate_anime_studio_sql(pairs: list[tuple[int, int]]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime_studio CASCADE;",
        "",
        "CREATE TABLE anime_studio (",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    studio_id INTEGER REFERENCES studio(studio_id),",
        "    PRIMARY KEY (anime_id, studio_id)",
        ");",
        "",
    ]
    for aid, sid in pairs:
        lines.append(
            f"INSERT INTO anime_studio (anime_id, licensor_id) VALUES ({aid}, {sid});"
        )
    return "\n".join(lines)

def anime_explicit_genre(animes: list[dict]) -> list[tuple[int, int]]:
    """Extrae pares (anime_id, explicit_genre_id) para la tabla junction."""
    pairs: set[tuple[int, int]] = set()
    for anime in animes:
        aid = anime["data"]["mal_id"]
        for e in anime["data"].get("explicit_genres", []):
            pairs.add((aid, e["mal_id"]))
    return sorted(pairs)

def generate_anime_explicit_genre_sql(pairs: list[tuple[int, int]]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime_explicit_genre CASCADE;",
        "",
        "CREATE TABLE anime_explicit_genre (",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    explicit_genre_id INTEGER REFERENCES explicit_genre(explicit_genre_id),",
        "    PRIMARY KEY (anime_id, explicit_genre_id)",
        ");",
        "",
    ]
    for aid, eid in pairs:
        lines.append(
            f"INSERT INTO anime_explicit_genre (anime_id, explicit_genre_id) VALUES ({aid}, {eid});"
        )
    return "\n".join(lines)

def anime_theme(animes: list[dict]) -> list[tuple[int, int]]:
    """Extrae pares (anime_id, theme_id) para la tabla junction."""
    pairs: set[tuple[int, int]] = set()
    for anime in animes:
        aid = anime["data"]["mal_id"]
        for t in anime["data"].get("themes", []):
            pairs.add((aid, t["mal_id"]))
    return sorted(pairs)

def generate_anime_theme_sql(pairs: list[tuple[int, int]]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime_theme CASCADE;",
        "",
        "CREATE TABLE anime_theme (",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    theme_id  INTEGER REFERENCES theme(theme_id),",
        "    PRIMARY KEY (anime_id, theme_id)",
        ");",
        "",
    ]
    for aid, tid in pairs:
        lines.append(
            f"INSERT INTO anime_theme (anime_id, theme_id) VALUES ({aid}, {tid});"
        )
    return "\n".join(lines)

def anime_demographic(animes: list[dict]) -> list[tuple[int, int]]:
    """Extrae pares (anime_id, demographic_id) para la tabla junction."""
    pairs: set[tuple[int, int]] = set()
    for anime in animes:
        aid = anime["data"]["mal_id"]
        for d in anime["data"].get("demographics", []):
            pairs.add((aid, d["mal_id"]))
    return sorted(pairs)

def generate_anime_demographic_sql(pairs: list[tuple[int, int]]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime_demographic CASCADE;",
        "",
        "CREATE TABLE anime_demographic (",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    demographic_id INTEGER REFERENCES demographic(demographic_id),",
        "    PRIMARY KEY (anime_id, demographic_id)",
        ");",
        "",
    ]
    for aid, did in pairs:
        lines.append(
            f"INSERT INTO anime_demographic (anime_id, demographic_id) "
            f"VALUES ({aid}, {did});"
        )
    return "\n".join(lines)

# =============================================================================
# TODO: Tabla relation
# =============================================================================
# Tipos de columnas:
#   relation_id SERIAL PK, anime_id INTEGER FK(anime), related_id INTEGER,
#   related_type TEXT, relation_type TEXT
#
# Fuente JSON: anime["data"]["relations"] -> lista de objetos con:
#   "relation": "Adaptation" (tipo de relacion)
#   "entry": [{"mal_id": ..., "type": "manga", ...}] (recursos relacionados)

# TODO: implementar extraccion y generacion SQL para relation
def extract_relation(animes: list[dict]) -> list[dict]:
    """Extrae relaciones de todos los animes."""
    relations = []
    i = 1

    for anime in animes:
        data = anime.get("data", {})
        anime_id = data.get("mal_id")
        relations_data = data.get("relations", [])

        for r in relations_data:
            relation_type = r.get("relation")
            entries = r.get("entry", [])

            for e in entries:
                relations.append({
                    "relation_id": i,
                    "anime_id": anime_id,
                    "related_id": e.get("mal_id"),
                    "related_type": e.get("type"),
                    "relation_type": relation_type
                })
                i += 1

    return relations

def generate_relation_sql(relation: list[dict]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS relation CASCADE;",
        "",
        "CREATE TABLE relation (",
        "    relation_id SERIAL PRIMARY KEY,",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    related_id INTEGER,",
        "    related_type TEXT,",
        "    relation_type TEXT",
        ");",
        "",
    ]
    for r in relation:
        lines.append(
            f"INSERT INTO relation (anime_id, related_id, related_type, relation_type)"
            f"VALUES ({val(r['anime_id'])}, {val(r['related_id'])}, {val(r['related_type'])}, {val(r['relation_type'])});"
        
        )
    return "\n".join(lines)
# =============================================================================
# TODO: Tablas streaming
# =============================================================================
# Tipos de columnas:
#   streaming:       streaming_id SERIAL PK, name TEXT UNIQUE
#   anime_streaming: anime_id INTEGER FK(anime), streaming_id INTEGER FK(streaming), url TEXT, PK(anime_id, streaming_id)
#
# Fuente JSON: anime["data"]["streaming"] -> [{"name": "...", "url": "..."}]
# Ojo: streaming usa SERIAL -> mapear nombre -> id igual que season/rating.

# TODO: implementar extraccion y generacion SQL para streaming y anime_streaming
def extract_streaming(animes: list[dict]) -> list[dict]:
    """Extrae servicios de streaming unicos de todos los animes."""
    streaming: dict[str, dict] = {}
    i = 1

    for anime in animes:
        data = anime.get("data", {}).get("streaming", [])

        for d in data:
            name = d.get("name")

            if name not in streaming:
                streaming[name] = {
                    "streaming_id": i,
                    "name": name
                }
                i += 1

    return streaming

def generate_streaming_sql(streaming: dict) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS streaming CASCADE;",
        "",
        "CREATE TABLE streaming (",
        "    streaming_id SERIAL PRIMARY KEY,",
        "    name TEXT UNIQUE",
        ");",
        "",
    ]
    for s in streaming.values():
        lines.append(
            f"INSERT INTO streaming (name) "
            f"VALUES ({val(s['name'])});"
        )

    return "\n".join(lines)

def extract_anime_streaming(animes: list[dict], streaming: dict) -> list[dict]:
    anime_streaming = []

    for anime in animes:
        data = anime.get("data", {}).get("streaming", [])
       
        for d in data:
            name = d.get("name")
            streaming_id = streaming.get(name, {}).get("streaming_id")

            if streaming_id is not None:
                anime_streaming.append({
                    "anime_id": anime.get("data", {}).get("mal_id"),
                    "streaming_id": streaming_id,
                    "url": d.get("url")
                })

    return anime_streaming

def generate_anime_streaming_sql(anime_streaming) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime_streaming CASCADE;",
        "",
        "CREATE TABLE anime_streaming (",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    streaming_id INTEGER REFERENCES streaming(streaming_id),",
        "    url TEXT,",
        "    PRIMARY KEY (anime_id, streaming_id)",
        ");",
        "",
    ]
    for an_st in anime_streaming:
        lines.append(
            f"INSERT INTO anime_streaming (anime_id, streaming_id, url) "
            f"VALUES ({val(an_st['anime_id'])}, {val(an_st['streaming_id'])}, {val(an_st['url'])});"
        )
    return "\n".join(lines)
# =============================================================================
# TODO: Tablas character, anime_character, voice_actor, character_voice_actor
# =============================================================================
# Datos en anime_characters.json (lista paralela a anime_full.json).
# Cada entrada tiene "data": lista de personajes con sus actores de voz.
#
# Tipos de columnas:
#   character:             character_id INTEGER PK, url TEXT, image_url TEXT, name TEXT
#   anime_character:       anime_id INTEGER FK(anime), character_id INTEGER FK(character), role TEXT, favorites INTEGER, PK(anime_id, character_id)
#   voice_actor:           voice_actor_id INTEGER PK, name TEXT, url TEXT, image_url TEXT, language TEXT
#   character_voice_actor: character_id INTEGER FK(character), voice_actor_id INTEGER FK(voice_actor), PK(character_id, voice_actor_id)
#
# Fuente JSON (anime_characters.json[i]["data"][j]):
#   "character": {"mal_id", "url", "images": {"jpg": {"image_url"}}, "name"}
#   "role": "Main" / "Supporting"
#   "favorites": int
#   "voice_actors": [{"person": {"mal_id", "url", "images": {"jpg": {"image_url"}}, "name"}, "language": "Japanese"}]

# TODO: implementar extraccion y generacion SQL
def extract_character(characters: list[dict]) -> list[dict]:
    characters_list = []

    for c in characters:
        data = c.get("data", [])

        for d in data:
            char = d["character"]

            characters_list.append({
                "character_id": char["mal_id"],
                "url": char["url"],
                "image_url": char["images"]["jpg"]["image_url"],
                "name": char["name"]
            })

    return characters_list

def generate_character_sql(characters) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS characters CASCADE;",
        "",
        "CREATE TABLE characters (",
        "    character_id INTEGER PRIMARY KEY,",
        "    url TEXT,",
        "    image_url TEXT,",
        "    name TEXT",
        ");",
        "",
    ]
    for c in characters:
        lines.append(
            f"INSERT INTO characters (character_id, url, image_url, name) "
            f"VALUES ({val(c['character_id'])}, {val(c['url'])}, {val(c['image_url'])}, {val(c['name'])});"
        )

    return "\n".join(lines)

def extract_anime_character(characters: list[dict]) -> list[dict]:
    anime_character = []

    for c in characters:
        data = c.get("data", [])

        for d in data:
            char = d["character"]

            anime_character.append({
                "anime_id": d["anime_id"],
                "character_id": char["mal_id"],
                "role": d["role"],
                "favorites": d["favorites"]
            })

    return anime_character

def generate_anime_character_sql(anime_character: list[tuple[int, int]]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime_character CASCADE;",
        "",
        "CREATE TABLE anime_character (",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    character_id INTEGER REFERENCES characters(character_id),",
        "    role TEXT,",
        "    favorites INTEGER,",
        "    PRIMARY KEY (anime_id, character_id)",
        ");",
        "",
    ]
    for ac in anime_character:
        lines.append(
            f"INSERT INTO anime_character (anime_id, character_id, role, favorites) "
            f"VALUES ({val(ac['anime_id'])}, {val(ac['character_id'])}, {val(ac['role'])}, {val(ac['favorites'])});"
        )
    return "\n".join(lines)

def extract_voice_actor(characters: list[dict]) -> dict[str, dict]:
    voice_actors = {}
    
    for c in characters:
        data = c.get("data", [])

        for d in data:
            for va in d.get("voice_actors", []):
                person = va.get("person", {})
                vid = person.get("mal_id")

                if vid not in voice_actors:
                    voice_actors[vid] = {
                        "voice_actor_id": vid,
                        "name": person.get("name"),
                        "image_url": person.get("images", {}).get("jpg", {}).get("image_url"),
                        "language": va.get("language")
                    }   

    return list(voice_actors.values())

def generate_voice_actors_sql(voice_actors) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS voice_actor CASCADE;",
        "",
        "CREATE TABLE voice_actor (",
        "    voice_actor_id INTEGER PRIMARY KEY,",
        "    name TEXT,",
        "    image_url TEXT,",
        "    language TEXT",
        ");",
        "",
    ]
    for v in voice_actors:
        lines.append(
            f"INSERT INTO voice_actor (voice_actor_id, name, image_url, language) "
            f"VALUES ({val(v['voice_actor_id'])}, {val(v['name'])}, {val(v['image_url'])}, {val(v['language'])});"
        )

    return "\n".join(lines)

def extract_character_voice_actor(characters: list[dict]) -> list[dict]:
    character_voice_actor = []

    for c in characters:
        data = c.get("data", [])

        for d in data:
            char = d["character"]

            for va in d.get("voice_actors", []):
                person = va.get("person", {})
                vid = person.get("mal_id")

                character_voice_actor.append({
                    "character_id": char["mal_id"],
                    "voice_actor_id": vid
                })

    return character_voice_actor

def generate_character_voice_actor_sql(character_voice_actor: list[tuple[int, int]]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS character_voice_actor CASCADE;",
        "",
        "CREATE TABLE character_voice_actor (",
        "    character_id INTEGER REFERENCES anime(anime_id),",
        "    voice_actor_id INTEGER REFERENCES characters(character_id),",
        "    PRIMARY KEY (anime_id, character_id)",
        ");",
        "",
    ]
    for ac in character_voice_actor:
        lines.append(
            f"INSERT INTO character_voice_actor (character_id, voice_actor_id) "
            f"VALUES ({val(ac['character_id'])}, {val(ac['voice_actor_id'])});"
        )
    return "\n".join(lines)

# =================================
# ============================================
# TODO: Tablas staff, anime_staff
# =============================================================================
# Datos en anime_staff.json (lista paralela a anime_full.json).
# Cada miembro puede tener multiples posiciones -> una fila por posicion.
#
# Tipos de columnas:
#   staff:       staff_id INTEGER PK, url TEXT, image_url TEXT, name TEXT
#   anime_staff: anime_id INTEGER FK(anime), staff_id INTEGER FK(staff), position TEXT, PK(anime_id, staff_id, position)
#
# Fuente JSON (anime_staff.json[i]["data"][j]):
#   "person": {"mal_id", "url", "images": {"jpg": {"image_url"}}, "name"}
#   "positions": ["Director", "Producer", ...]
#   "anime_id": int

# TODO: implementar extraccion y generacion SQL
def extract_staff(staff: list[dict]) -> list[dict]:
    staff_list = []

    for s in staff:
        data = s.get("data", [])

        for d in data:
            st = d["person"]

            staff_list.append({
                "staff_id": st["mal_id"],
                "url": st["url"],
                "image_url": st["images"]["jpg"]["image_url"],
                "name": st["name"]
            })

    return staff_list

def generate_staff_sql(staff) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS staff CASCADE;",
        "",
        "CREATE TABLE staff (",
        "    staff_id INTEGER PRIMARY KEY,",
        "    url TEXT,",
        "    image_url TEXT,",
        "    name TEXT",
        ");",
        "",
    ]
    for s in staff:
        lines.append(
            f"INSERT INTO staff (staff_id, url, image_url, name) "
            f"VALUES ({val(s['staff_id'])}, {val(s['url'])}, {val(s['image_url'])}, {val(s['name'])});"
        )

    return "\n".join(lines)

def extract_anime_staff(staff: list[dict]) -> list[dict]:
    anime_staff = []

    for s in staff:
        data = s.get("data", [])

        for d in data:

            for position in d.get("positions", []):
                anime_staff.append({
                    "anime_id": d["anime_id"],
                    "staff_id": d["person"]["mal_id"],
                    "position": position
                })

    return anime_staff

def generate_anime_staff_sql(anime_staff: list[tuple[int, int, str]]) -> str:
    lines: list[str] = [
        "DROP TABLE IF EXISTS anime_staff CASCADE;",
        "",
        "CREATE TABLE anime_staff (",
        "    anime_id INTEGER REFERENCES anime(anime_id),",
        "    staff_id INTEGER REFERENCES staff(staff_id),",
        "    position TEXT,",
        "    PRIMARY KEY (anime_id, staff_id, position)",
        ");",
        "",
    ]
    for as_ in anime_staff:
        lines.append(
            f"INSERT INTO anime_staff (anime_id, staff_id, position) "
            f"VALUES ({val(as_['anime_id'])}, {val(as_['staff_id'])}, {val(as_['position'])});"
        )
    return "\n".join(lines)

# =============================================================================
# Script principal
# =============================================================================

if __name__ == "__main__":
    # Cargar todos los datos en memoria
    animes, characters, staff = load_all_data()

    # --- Ejemplo: genre ---
    genres = extract_genres(animes)
    
    # TODO: extraer datos de todas las demas tablas
    seasons = extract_season(animes)
    ratings = extract_rating(animes)
    types = extract_type(animes)
    sources = extract_source(animes)
    titles = extract_titles(animes)
    animes_list = extract_anime(animes, seasons, ratings, types, sources)
    titles = extract_titles(animes)
    producers =extract_producer(animes)
    licensors = extract_licensor(animes)
    studios = extract_studio(animes)
    explicit_genres = extract_explicit_genres(animes)
    themes = extract_themes(animes)
    demographics = extract_demographics(animes)

    anime_genre_pairs = extract_anime_genre(animes)
    anime_producers_pairs = anime_producer(animes)
    anime_licensors_pairs = anime_licensor(animes)
    anime_studios_pairs = anime_studio(animes)
    anime_explicit_genres_pairs = anime_explicit_genre(animes)
    anime_themes_pairs = anime_theme(animes)
    anime_demographics_pairs = anime_demographic(animes)

    relations = extract_relation(animes)

    streaming = extract_streaming(animes)
    anime_streaming = extract_anime_streaming(animes, streaming)


    characters_list = extract_character(characters)
    anime_character = extract_anime_character(characters)
    voice_actors = extract_voice_actor(characters)
    character_voice_actor = extract_character_voice_actor(characters)

    staff_list = extract_staff(staff)
    anime_staff = extract_anime_staff(staff)

    print(f"\nGeneros extraidos: {len(genres)}")
    print(f"\nPares anime-genero: {len(anime_genre_pairs)}")

    print(f"\nTemporadas extraidas: {len(seasons)}")
    print(f"\n Ratings extraidos: {len(ratings)}")
    print(f"\n Types extraidos: {len(types)}")
    print(f"\n Sources extraidos: {len(sources)}")
    print(f"\n Animes extraidos: {len(animes_list)}")
    print(f"\n Titulos extraidos: {len(titles)}")
    print(f"\n Productores extraidos: {len(producers)}")
    print(f"\n Licenciatarios extraidos: {len(licensors)}")
    print(f"\n Estudios extraidos: {len(studios)}")
    print(f"\n Generos explcitos extraidos: {len(explicit_genres)}")
    print(f"\n Temas extraidos: {len(themes)}")
    print(f"\n Demografías extraidos: {len(demographics)}")

    print(f"\nPares anime-productores: {len(anime_producers_pairs)}")
    print(f"\nPares anime-licenciatarios: {len(anime_licensors_pairs)}")
    print(f"\nPares anime-estudios: {len(anime_studios_pairs)}")
    print(f"\nPares anime-genero_explicito: {len(anime_explicit_genres_pairs)}")
    print(f"\nPares anime-temas: {len(anime_themes_pairs)}")
    print(f"\nPares anime-demografías: {len(anime_demographics_pairs)}")

    print(f"\nRelaciones extraidos: {len(relations)}")

    print(f"\nStreaming extraidos: {len(streaming)}")
    print(f"\nanime-streaming extraidos: {len(anime_streaming)}")

    print(f"\nPersonajes extraidos: {len(characters_list)}")
    print(f"\nanime-personajes: {len(anime_character)}")
    print(f"\nVoice actors extraidos: {len(voice_actors)}")
    print(f"\npersonajes-voice_actors: {len(character_voice_actor)}")
    print(f"\nStaff extraidos: {len(staff_list)}")
    print(f"\nanime-staff: {len(anime_staff)}")

    # Generar dump.sql
    # IMPORTANTE: deben respetar el orden de creacion segun las dependencias
    # de claves foraneas. Si la tabla B tiene un FK que apunta a la tabla A,
    # entonces A debe crearse ANTES que B. Revisen el esquema y piensen bien
    # el orden antes de armar el dump.
    dump_parts: list[str] = []
    dump_parts.append(generate_genre_sql(genres))
    dump_parts.append(generate_anime_genre_sql(anime_genre_pairs))
    # TODO: agregar todas las demas tablas
    dump_parts.append(generate_season_sql(seasons))
    dump_parts.append(generate_rating_sql(ratings))
    dump_parts.append(generate_type_sql(types))
    dump_parts.append(generate_source_sql(sources))

    dump_parts.append(generate_anime_sql(animes_list))

    dump_parts.append(generate_title_sql(titles))

    dump_parts.append(generate_producers_sql(producers))
    dump_parts.append(generate_licensors_sql(licensors))
    dump_parts.append(generate_studios_sql(studios))
    dump_parts.append(generate_explicit_genres_sql(explicit_genres))
    dump_parts.append(generate_themes_sql(themes))
    dump_parts.append(generate_demographics_sql(demographics))

    dump_parts.append(generate_anime_genre_sql(anime_producers_pairs))
    dump_parts.append(generate_anime_licensor_sql(anime_licensors_pairs))
    dump_parts.append(generate_anime_studio_sql(anime_studios_pairs))
    dump_parts.append(generate_anime_explicit_genre_sql(anime_explicit_genres_pairs))
    dump_parts.append(generate_anime_theme_sql(anime_themes_pairs))
    dump_parts.append(generate_anime_demographic_sql(anime_demographics_pairs))

    dump_parts.append(generate_relation_sql(relations))

    dump_parts.append(generate_streaming_sql(streaming))
    dump_parts.append(generate_anime_streaming_sql(anime_streaming))

    dump_parts.append(generate_character_sql(characters_list))
    dump_parts.append(generate_anime_character_sql(anime_character))
    dump_parts.append(generate_voice_actors_sql(voice_actors))
    dump_parts.append(generate_character_voice_actor_sql(character_voice_actor))

    dump_parts.append(generate_staff_sql(staff_list))
    dump_parts.append(generate_anime_staff_sql(anime_staff))
    
    with open("dump.sql", "w", encoding="utf-8") as f:
        f.write("\n\n".join(dump_parts) + "\n")

    print(f"\ndump.sql generado ({len(dump_parts)} tablas)")
