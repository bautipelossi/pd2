import pandas as pd
import os

# Ruta de tu archivo original
ARCHIVO_ENTRADA = r"C:\Users\palon\Downloads\dataset_trafico_vis_ready.csv"

# Crear la ruta de salida reemplazando la extensión .csv por .parquet
ARCHIVO_SALIDA = os.path.splitext(ARCHIVO_ENTRADA)[0] + '.parquet'

try:
    # Leer el archivo CSV
    print(f"Leyendo el archivo CSV desde: {ARCHIVO_ENTRADA}")
    df = pd.read_csv(ARCHIVO_ENTRADA)

    # Guardar el archivo en formato Parquet
    print("Convirtiendo y guardando en formato Parquet...")
    df.to_parquet(ARCHIVO_SALIDA, engine='pyarrow', index=False)
    
    print(f"¡Éxito! Archivo guardado correctamente en: {ARCHIVO_SALIDA}")

except FileNotFoundError:
    print("Error: No se pudo encontrar el archivo CSV en la ruta especificada.")
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}")