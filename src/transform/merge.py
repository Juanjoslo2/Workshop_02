import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def limpiar_y_preparar_columna(df, columna_origen, columna_destino):
    if columna_origen in df.columns:
        # Extraer solo el primer artista si hay múltiples separados por coma
        df[columna_destino] = df[columna_origen].astype(str).str.split(',').str[0].str.lower().str.strip()
        return True
    else:
        logging.error(f"La columna de origen '{columna_origen}' no existe en el DataFrame.")
        return False

def fill_null_values(df, columns, value):
    for column in columns:
        if column in df.columns:
            df[column] = df[column].fillna(value)
        else:
            logging.warning(f"La columna '{column}' para rellenar valores nulos no fue encontrada.")

def drop_columns(df, columns):
    cols_to_drop = [col for col in columns if col in df.columns]
    if cols_to_drop:
        df.drop(columns=cols_to_drop, inplace=True)
        logging.info(f"Columnas eliminadas: {cols_to_drop}")
    else:
        logging.warning(f"Ninguna de las columnas especificadas para eliminar fue encontrada: {columns}")

def merging_datasets(api_df: pd.DataFrame, spotify_df: pd.DataFrame, grammys_df: pd.DataFrame) -> pd.DataFrame:

    logging.info("Iniciando la fusión de datasets.")

    if spotify_df.empty or grammys_df.empty or api_df.empty:
        logging.error("Uno o más DataFrames de entrada están vacíos. No se puede continuar con la fusión.")
        return pd.DataFrame()

    logging.info(f"Dataset Spotify inicial tiene {spotify_df.shape[0]} filas y {spotify_df.shape[1]} columnas.")
    logging.info(f"Dataset Grammys inicial tiene {grammys_df.shape[0]} filas y {grammys_df.shape[1]} columnas.")
    logging.info(f"Dataset API inicial tiene {api_df.shape[0]} filas y {api_df.shape[1]} columnas.")

    try:
        spotify_df_copy = spotify_df.copy()
        grammys_df_copy = grammys_df.copy()
        api_df_copy = api_df.copy()

        if not limpiar_y_preparar_columna(spotify_df_copy, "track_name", "track_name_clean"):
             return pd.DataFrame()
        if not limpiar_y_preparar_columna(grammys_df_copy, "nominee", "nominee_clean"):
             return pd.DataFrame()

        logging.info("Realizando primera fusión (Spotify y Grammys) por nombre de canción/nominado.")
        df_merged_step1 = spotify_df_copy.merge(
            grammys_df_copy,
            how="left",
            left_on="track_name_clean",
            right_on="nominee_clean",
            suffixes=("", "_grammys")
        )

        fill_columns_grammys = ["title", "category"]
        fill_null_values(df_merged_step1, fill_columns_grammys, "No aplicable")
        fill_column_grammys = ["is_nominated"]
        fill_null_values(df_merged_step1, fill_column_grammys, False)

        columns_drop_step1 = ["year", "artist_grammys", "nominee", "nominee_clean", "track_name_clean"]
        drop_columns(df_merged_step1, columns_drop_step1)

        logging.info(f"Primera fusión completada. DataFrame intermedio tiene {df_merged_step1.shape[0]} filas.")


        if not limpiar_y_preparar_columna(df_merged_step1, "artists", "artist_clean"):
             return pd.DataFrame()
        if not limpiar_y_preparar_columna(api_df_copy, "artist", "artist_api_clean"):
             return pd.DataFrame()


        logging.info("Realizando segunda fusión (Resultado anterior y API) por nombre de artista.")
        df_merged_final = df_merged_step1.merge(
            api_df_copy[['artist_api_clean', 'country', 'type']], # Seleccionar solo columnas necesarias de API
            how="left",
            left_on="artist_clean",
            right_on="artist_api_clean",
            suffixes=("", "_api")
        )


        if 'country_api' in df_merged_final.columns:
             df_merged_final.rename(columns={'country_api': 'country'}, inplace=True)
        if 'type_api' in df_merged_final.columns:
             df_merged_final.rename(columns={'type_api': 'type'}, inplace=True)


        fill_columns_api = ["country", "type"]
        fill_null_values(df_merged_final, fill_columns_api, "Desconocido")


        columns_drop_final = ["artist_clean", "artist_api_clean", "artist"]
        drop_columns(df_merged_final, columns_drop_final)


        df_merged_final.reset_index(drop=True, inplace=True)
        df_merged_final.insert(0, 'id', range(len(df_merged_final)))

        logging.info(f"Proceso de fusión completado. El DataFrame final tiene {df_merged_final.shape[0]} filas y {df_merged_final.shape[1]} columnas.")

        return df_merged_final

    except KeyError as e:
        logging.error(f"Error de clave durante el proceso de fusión: No se encontró la columna '{e}'. Revisa los nombres de las columnas en los DataFrames.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado durante el proceso de fusión: {e}")
        return pd.DataFrame()