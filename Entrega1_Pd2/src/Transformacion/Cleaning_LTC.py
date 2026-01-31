import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]   # ajusta si tu estructura cambia
DATA_DIR = PROJECT_ROOT / "datos" / "crudos" / "sampled_nyc_yellow_taxi_2023.csv"

# =============
# Exploración a grandes rasgos de los datos
# =============
def explore_data(df):

    """Explorar los datos descargados"""
    if df is None or df.empty:
        print("No hay datos para explorar")
        return

    print("\n" + "=" * 50)
    print("🔎 EXPLORACIÓN DE DATOS")
    print("=" * 50)

    print("\n📏 Nº de filas del dataframe")
    nrows, ncols = df.shape
    print(nrows)

    print("\n📄 Primeras 5 filas:")
    print(df.head())

    print("\n📊 Tipos de datos:")
    print(df.dtypes)

    print("\n🧮 Estadísticas (columnas numéricas):")
    print(df.describe())

    print("\n⚠️ Valores nulos por columna:")
    print(df.isnull().sum())

    print("\n🔢 Valores distintos por columna:")
    print(df[["vendorid", "pulocationid", "dolocationid", "payment_type"]].nunique())


# =============
# Consultas e info básicas
# =============
def basic_queries(df):
    print("\n" + "=" * 50)
    print("📈 CONSULTAS ÚTILES")
    print("=" * 50)

    if "trip_distance" in df.columns:
        df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")
        print("\n📍 Top 5 viajes más largos:")
        print(
            df.nlargest(5, "trip_distance")[
                ["trip_distance", "fare_amount", "total_amount"]
            ]
        )

    if "vendorid" in df.columns:
        print("\n🏢 Viajes por vendor:")
        print(df["vendorid"].value_counts())

    if "payment_type" in df.columns:
        print("\n💳 Distribución de tipos de pago:")
        print(df["payment_type"].value_counts())


def main():

    print("=" * 40)
    print("🚕 LIMPIEZA Y EXPLORACIÓN - NYC TAXI")
    print("=" * 40)

    if not DATA_DIR.exists():
        print(f"❌ No se ha encontrado el archivo: {DATA_DIR}")
        return

    print(f"\n📂 Cargando datos desde {DATA_DIR}")

    df = pd.read_csv(DATA_DIR, sep = ',')

    explore_data(df)
    basic_queries(df)


if __name__ == "__main__":
    main()