import os
import time
import logging
import requests
import pandas as pd
from tqdm import tqdm

MUSICBRAINZ_ENDPOINT = "https://musicbrainz.org/ws/2/artist/"
HEADERS = {
    "User-Agent": "workshop2/1.0 (ejemplo@gmail.com)"
}

ARTISTS_CSV = ('data/artists.csv')
RETRY_LIMIT = 3
BATCH_SIZE = 50
RESULTS_PER_PAGE = 100
ARTIST_LIMIT = 5000

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def limpiar_nombre(nombre: str) -> str:
    if pd.isna(nombre) or not nombre.strip():
        return None
    nombre = nombre.replace("\\", "")
    nombre = nombre.replace('"', '')
    nombre = nombre.replace("'", "")
    nombre = nombre.replace("/", " ")
    nombre = nombre.replace("&", "and")
    nombre = nombre.strip()
    return nombre

def _cargar_y_limpiar_artistas(ruta_csv: str) -> list:
    df = pd.read_csv(ruta_csv, header=None, names=["raw"])
    nombres_limpios = [limpiar_nombre(nombre) for nombre in df["raw"]]
    
    artistas_unicos = sorted(set([nombre for nombre in nombres_limpios if nombre]))
    artistas_limitados = artistas_unicos[:ARTIST_LIMIT] if ARTIST_LIMIT else artistas_unicos

    logging.info(f"✅ Total artistas únicos procesados: {len(artistas_limitados)}")
    return artistas_limitados

def _consultar_musicbrainz(artistas: list) -> list: # Cambiado nombre del parámetro
    resultados = []
    logging.info("🎧 Consultando MusicBrainz...")
    
    with tqdm(total=len(artistas), desc="🔎 MusicBrainz") as pbar:
        for i in range(0, len(artistas), BATCH_SIZE):
            lote_artistas = artistas[i:i + BATCH_SIZE]
            query = ' OR '.join([f'artist:"{artista}"' for artista in lote_artistas])
            page = 1
            while True:
                info_lote = _buscar_artista_musicbrainz_lote(query, page)

                if info_lote:
                    resultados.extend(info_lote)
                else:
                    pass

                if not info_lote or len(info_lote) < RESULTS_PER_PAGE:
                    break

                page += 1
                time.sleep(1.1)
            pbar.update(len(lote_artistas))

    return resultados

def _buscar_artista_musicbrainz_lote(query: str, page: int) -> list:
    params = {
        "query": query,
        "fmt": "json",
        "offset": (page - 1) * RESULTS_PER_PAGE,
        "limit": RESULTS_PER_PAGE
    }

    for intento in range(RETRY_LIMIT):
        try:
            response = requests.get(MUSICBRAINZ_ENDPOINT, headers=HEADERS, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get("artists"):
                resultados_pagina = []
                for artista in data["artists"]:
                    resultados_pagina.append({
                        "artist": artista.get("name", ""),
                        "country": artista.get("country", ""),
                        "type": artista.get("type", ""),
                        "disambiguation": artista.get("disambiguation", ""),
                        "life_begin": artista.get("life-span", {}).get("begin", ""),
                        "life_end": artista.get("life-span", {}).get("end", "")
                    })
                return resultados_pagina
            else:
                return []

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error al consultar lote (Intento {intento + 1}/{RETRY_LIMIT}) - Página {page}: {e}")
            if intento == RETRY_LIMIT - 1:
                 logging.error(f"❌ Falló consulta del lote tras {RETRY_LIMIT} intentos.")
            time.sleep(2 * (intento + 1))
        except Exception as e:
            logging.error(f"❌ Error inesperado al consultar lote (Página {page}): {e}")
            return []

    return []

def extract_musicbrainz() -> pd.DataFrame:

    artistas_a_consultar = _cargar_y_limpiar_artistas(ARTISTS_CSV)

    resultados = _consultar_musicbrainz(artistas_a_consultar)
    columnas = ["artist", "country", "type", "disambiguation", "life_begin", "life_end"]

    logging.info("✅ Consulta completada. Resultados obtenidos:")

    df_resultado = pd.DataFrame(resultados, columns=columnas)

    return df_resultado