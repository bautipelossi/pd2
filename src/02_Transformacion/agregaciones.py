import pandas as pd
import numpy as np
from pathlib import Path

"""
    En este Script se hace un único dataset en el que se combinan los datos de 
    los taxis con los de Uber.
    
    CORRECCIÓN CRÍTICA: Se agrega por FECHA EXACTA (pickup_date) en lugar de 
    solo por día de la semana para evitar sumar todo el semestre.
"""

BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]
DATA_DIR = PROJECT_ROOT / "Entrega1_Pd2" / "datos" / "limpios"

FHV_PATH = DATA_DIR / "fhv_2023_clean.parquet"
YLC_PATH = DATA_DIR / "nyc_taxi_clean.parquet"
OUTPUT_PATH = DATA_DIR / "resumen_zona_hora.parquet"

def cargar_y_normalizar():
    print("Leyendo parquets...")
    df_fhv = pd.read_parquet(FHV_PATH)
    df_ylc = pd.read_parquet(YLC_PATH)

    if "tpep_pickup_datetime" in df_ylc.columns:
        df_ylc = df_ylc.rename(columns={"tpep_pickup_datetime": "pickup_datetime"})

    df_fhv["pickup_datetime"] = pd.to_datetime(df_fhv["pickup_datetime"], errors="coerce")
    df_ylc["pickup_datetime"] = pd.to_datetime(df_ylc["pickup_datetime"], errors="coerce")

    df_fhv = df_fhv.dropna(subset=["pickup_datetime", "pulocationid"])
    df_ylc = df_ylc.dropna(subset=["pickup_datetime", "pulocationid"])

    df_fhv["tipo_servicio"] = "FHV"
    df_ylc["tipo_servicio"] = "YLC"

    df_fhv = df_fhv[["pickup_datetime", "pulocationid", "tipo_servicio"]]
    df_ylc = df_ylc[["pickup_datetime", "pulocationid", "tipo_servicio"]]

    df_total = pd.concat([df_fhv, df_ylc], ignore_index=True)

    # --- CAMBIO CLAVE: Extraemos la fecha exacta ---
    df_total["pickup_date"] = df_total["pickup_datetime"].dt.date
    df_total["pickup_hour"] = df_total["pickup_datetime"].dt.hour.astype(int)
    
    # Mantenemos el day_of_week para el modelo (1=Dom... 7=Sab)
    df_total["day_of_week"] = ((df_total["pickup_datetime"].dt.dayofweek + 1) % 7) + 1
    df_total["pulocationid"] = df_total["pulocationid"].astype(int)

    return df_total

def agregar_por_zona_hora(df_total: pd.DataFrame) -> pd.DataFrame:
    print("Agregando por (pulocationid, pickup_date, pickup_hour, tipo_servicio)...")

    # Agrupamos por fecha exacta en lugar de solo por el número del día de la semana
    agg = (
        df_total
        .groupby(["pulocationid", "pickup_date", "day_of_week", "pickup_hour", "tipo_servicio"])
        .size()
        .reset_index(name="viajes")
    )

    pivot = (
        agg
        .pivot(index=["pulocationid", "pickup_date", "day_of_week", "pickup_hour"], columns="tipo_servicio", values="viajes")
        .fillna(0)
        .reset_index()
    )

    if "FHV" not in pivot.columns: pivot["FHV"] = 0
    if "YLC" not in pivot.columns: pivot["YLC"] = 0

    pivot["FHV"] = pivot["FHV"].astype(int)
    pivot["YLC"] = pivot["YLC"].astype(int)
    pivot["demanda_viajes"] = pivot["FHV"] + pivot["YLC"]

    # Para el modelo Spark, renombramos pickup_date a date_only para compatibilidad
    pivot = pivot.rename(columns={"pickup_date": "date_only"})
    
    pivot = pivot.sort_values(["date_only", "pickup_hour", "pulocationid"]).reset_index(drop=True)
    return pivot

def main():
    print("Generando resumen_zona_hora.parquet corregido...")
    df_total = cargar_y_normalizar()
    resumen = agregar_por_zona_hora(df_total)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    resumen.to_parquet(OUTPUT_PATH, index=False)
    print("¡Listo! Ya tienes el dataset preparado correctamente.")

if __name__ == "__main__":
    main()