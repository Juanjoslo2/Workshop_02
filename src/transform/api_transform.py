import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def transform_artist_data(df_input):
    if df_input.empty:
        logging.warning("El DataFrame de entrada está vacío. No se aplicarán transformaciones.")
        return pd.DataFrame()

    try:
        logging.info(f"Iniciando transformación de datos. Forma inicial del DataFrame: {df_input.shape}")

        df_processed = df_input.copy()

        required_columns = ['artist', 'country', 'type']
        if not all(col in df_processed.columns for col in required_columns):
             missing = [col for col in required_columns if col not in df_processed.columns]
             logging.error(f"Error: Faltan las columnas requeridas en el DataFrame de entrada: {missing}")
             return pd.DataFrame()


        original_rows = df_processed.shape[0]
        df_processed.dropna(subset=['artist'], inplace=True)

        df_processed = df_processed[df_processed['artist'].str.strip() != '']
        rows_dropped = original_rows - df_processed.shape[0]
        if rows_dropped > 0:
            logging.info(f"Se eliminaron {rows_dropped} filas sin valor en 'artist'.")


        df_processed['country'] = df_processed['country'].fillna("Desconocido")
        df_processed['type'] = df_processed['type'].fillna("Otro")
        logging.info("Valores faltantes imputados en 'country' (con 'Desconocido') y 'type' (con 'Otro').")


        df_processed['type'] = df_processed['type'].str.title()
        logging.info("Columna 'type' estandarizada a formato Título.")


        final_columns = ['artist', 'country', 'type']
        df_processed = df_processed[final_columns]
        logging.info(f"Seleccionadas las columnas finales: {final_columns}")


        logging.info(f"Transformación de datos completada. Forma final del DataFrame: {df_processed.shape}")
        return df_processed

    except KeyError as e:
        logging.error(f"Error de clave durante la transformación: No se encontró la columna esperada '{e}'.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado durante la transformación de datos: {e}")
        return pd.DataFrame()