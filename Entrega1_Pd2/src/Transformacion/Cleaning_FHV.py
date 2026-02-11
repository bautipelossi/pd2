"""
Cleaning_FHV.py
----------------
Limpieza del dataset High Volume FHV Trip Data (NYC 2023)
Procesamiento y guardado en csv
Proyecto de Datos II
"""

import pandas as pd
from pathlib import Path

print("=== Cleaning_FHV.py EJECUTADO ===")
print("Archivo:", __file__)

# =====================
# Paths
# =====================
BASE_DIR = Path(__file__).resolve().parents[2]

DATA_RAW = BASE_DIR / "datos" / "crudos"
DATA_PROCESSED = BASE_DIR / "datos" / "procesados"

INPUT_FILE = DATA_RAW / "nyc_fhv_2023_sampled.csv"
OUTPUT_FILE = DATA_PROCESSED / "fhv_2023_clean.csv"

# =====================
# Cleaning
# =====================
NUMERIC_COLS = [
    "pulocationid",
    "dolocationid",
    "trip_miles",
    "base_passenger_fare",
    "tolls",
    "tips",
    "driver_pay",
]

def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Fechas
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

    # Duración
    df["trip_duration_min"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds() / 60

    df = df[df["trip_duration_min"] > 0]

    # Conversión numérica
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filtros básicos
    df = df[df["trip_miles"] >= 0]
    df = df[df["base_passenger_fare"] >= 0]
    df = df[df["driver_pay"] >= 0]

    return df

# =====================
# Main
# =====================
OUTPUT_DIR = DATA_PROCESSED / "fhv_2023_clean_parquet"

def main():
    print(">>> Entré a main() <<<")

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    chunksize = 1_000_000
    total_rows = 0

    print("Iniciando limpieza FHV por chunks (PARQUET)...")

    for i, chunk in enumerate(pd.read_csv(INPUT_FILE, chunksize=chunksize)):
        print(f"Procesando chunk {i}")

        clean = clean_chunk(chunk)
        total_rows += len(clean)

        output_file = OUTPUT_DIR / f"part_{i:03d}.parquet"
        clean.to_parquet(output_file, index=False)

    print("Limpieza finalizada")
    print(f"Filas finales: {total_rows}")
    print(f"Archivos generados en: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()

#unificación parquet
import pandas as pd
from pathlib import Path

BASE_DIR = Path(r"C:\Users\Bauti\pd2\Entrega1_Pd2")

PARQUET_DIR = BASE_DIR / "datos" / "procesados" / "fhv_2023_clean_parquet"
OUTPUT_FILE = BASE_DIR / "datos" / "procesados" / "fhv_2023_clean.parquet"

print("Leyendo parquet por chunks...")
df = pd.read_parquet(PARQUET_DIR)

print("Guardando parquet unificado...")
df.to_parquet(OUTPUT_FILE, index=False)

print("Dataset final:", OUTPUT_FILE)
print("Filas:", len(df))

