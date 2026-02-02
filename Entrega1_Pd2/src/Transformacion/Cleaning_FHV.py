"""
Cleaning_FHV.py
----------------
Limpieza del dataset High Volume FHV Trip Data (NYC 2023)
Procesamiento por chunks y guardado en CSV
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
def main():
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    chunksize = 1_000_000
    first = True
    total_rows = 0

    print("Iniciando limpieza FHV por chunks (CSV)...")

    for i, chunk in enumerate(pd.read_csv(INPUT_FILE, chunksize=chunksize)):
        print(f"Procesando chunk {i}")

        clean = clean_chunk(chunk)
        total_rows += len(clean)

        clean.to_csv(
            OUTPUT_FILE,
            mode="w" if first else "a",
            header=first,
            index=False
        )

        first = False

    print("Limpieza finalizada")
    print(f"Filas finales: {total_rows}")
    print(f"Archivo generado: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
