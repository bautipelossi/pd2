# scripts/download_nyc_taxi.py
import requests
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import sys


# Ruta al archivo actual (LTC.py)
BASE_DIR = Path(__file__).resolve()

# Subimos niveles hasta Entrega1_Pd2
PROJECT_ROOT = BASE_DIR.parents[2]

# Ruta final a datos/crudos
DATA_CRUDOS_DIR = PROJECT_ROOT / "datos" / "crudos"


def download_taxi_data_by_date(
    start_date: str,
    end_date: str,
    limit: int = 50000
) -> pd.DataFrame:
    """
    Descarga datos de NYC Taxi para un rango de fechas usando paginación.
    """
    url = "https://data.cityofnewyork.us/resource/4b4i-vvec.json"
    offset = 0
    chunks = []

    print(f"📅 Descargando desde {start_date} hasta {end_date}")

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$where": (
                f"tpep_pickup_datetime >= '{start_date}' "
                f"AND tpep_pickup_datetime < '{end_date}'"
            )
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if not data:
            break

        df_chunk = pd.DataFrame(data)

        # Convertir fechas
        df_chunk["tpep_pickup_datetime"] = pd.to_datetime(
            df_chunk["tpep_pickup_datetime"], errors="coerce"
        )
        df_chunk["tpep_dropoff_datetime"] = pd.to_datetime(
            df_chunk["tpep_dropoff_datetime"], errors="coerce"
        )

        chunks.append(df_chunk)
        offset += limit

        print(f"   → {offset} filas descargadas")

    return pd.concat(chunks, ignore_index=True)


def save_monthly_data(df: pd.DataFrame, year: int, month: int) -> Path:
    DATA_CRUDOS_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"nyc_taxi_{year}_{month:02d}.csv"
    path = DATA_CRUDOS_DIR / filename

    df.to_csv(path, index=False)
    print(f"💾 Guardado: {path}")

    return path


def combine_dataframes(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    print("🔗 Uniendo todos los DataFrames...")
    return pd.concat(dfs, ignore_index=True)


def main():
    year = 2023
    all_dfs = []

    for month in range(1, 13):
        start_date = f"{year}-{month:02d}-01T00:00:00"

        if month == 12:
            end_date = f"{year + 1}-01-01T00:00:00"
        else:
            end_date = f"{year}-{month + 1:02d}-01T00:00:00"

        df_month = download_taxi_data_by_date(start_date, end_date)

        if not df_month.empty:
            save_monthly_data(df_month, year, month)
            all_dfs.append(df_month)

    # Unir todo en un solo DataFrame
    full_df = combine_dataframes(all_dfs)

    # Guardar dataset completo
    full_path = DATA_CRUDOS_DIR / f"nyc_taxi_{year}_full.csv"
    full_df.to_csv(full_path, index=False)

    print(f"\n✅ Dataset completo guardado en: {full_path}")
    print(f"📊 Total filas: {len(full_df)}")


if __name__ == "__main__":
    main()