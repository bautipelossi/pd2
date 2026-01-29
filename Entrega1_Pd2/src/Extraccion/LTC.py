# scripts/download_nyc_taxi.py
import requests
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import sys


def download_nyc_taxi_data(limit=1000, save=True, data_dir = "datos/crudos"):
    """
    Descarga datos de la API de NYC Taxi de forma simple

    Args:
        limit: Número de registros a descargar
        save: Guardar en archivo local
        data_dir: Directorio de los datos crudos donde se guardará
    """
    url = "https://data.cityofnewyork.us/resource/4b4i-vvec.json"

    print(f"🔍 Conectando a la API...")
    print(f"📊 URL: {url}")
    print(f"📈 Límite: {limit} registros")

    #Aegurarnos que existe lla carpeta donde vamos a guardar los datos
    raw_dir = Path(data_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Hacer la petición
        params = {"$limit": limit}
        response = requests.get(url, params=params)
        response.raise_for_status()

        # Convertir a JSON
        data = response.json()

        print(f"✅ Descargados {len(data)} registros")

        if not data:
            print("⚠️  ¡Cuidado! La API devolvió 0 registros")
            return None

        # Convertir a DataFrame de pandas
        df = pd.DataFrame(data)

        # Mostrar información básica
        print(f"\n📋 Información del dataset:")
        print(f"   • Registros: {len(df)}")
        print(f"   • Columnas: {len(df.columns)}")
        print(f"   • Periodo: {df['tpep_pickup_datetime'].min()} a {df['tpep_pickup_datetime'].max()}")

        if save:
            # Crear nombre de archivo con timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"nyc_taxi_data_{timestamp}.parquet"

            # Guardar en diferentes formatos
            df.to_parquet(filename, index=False)
            print(f"\n💾 Datos guardados como: {filename}")

            # También guardar como CSV para fácil visualización
            csv_name = f"nyc_taxi_data_{timestamp}.csv"
            df.to_csv(csv_name, index=False)
            print(f"💾 CSV guardado como: {csv_name}")

        return df

    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def explore_data(df):
    """Explorar los datos descargados"""
    if df is None or df.empty:
        print("No hay datos para explorar")
        return

    print("\n" + "=" * 50)
    print("🔎 EXPLORACIÓN DE DATOS")
    print("=" * 50)

    # Mostrar primeras filas
    print("\n📄 Primeras 5 filas:")
    print(df.head())

    # Información de tipos de datos
    print("\n📊 Tipos de datos:")
    print(df.dtypes)

    # Estadísticas básicas
    print("\n🧮 Estadísticas (columnas numéricas):")
    print(df.describe())

    # Valores nulos
    print("\n⚠️  Valores nulos por columna:")
    print(df.isnull().sum())

    # Columnas con fechas
    date_cols = [col for col in df.columns if 'datetime' in col.lower()]
    if date_cols:
        print(f"\n📅 Columnas de fecha/hora: {date_cols}")

        # Convertir a datetime
        for col in date_cols:
            df[col] = pd.to_datetime(df[col])
            print(f"   • {col}: {df[col].min()} → {df[col].max()}")


# Ejecutar si se llama directamente
if __name__ == "__main__":
    print("🚕 NYC TAXI DATA DOWNLOADER")
    print("=" * 40)

    # Descargar datos de prueba
    data = download_nyc_taxi_data(limit=1000, save=True)

    if data is not None:
        # Explorar datos
        explore_data(data)

        # Mostrar algunas consultas útiles
        print("\n" + "=" * 50)
        print("📈 CONSULTAS ÚTILES")
        print("=" * 50)

        # Top 5 viajes más largos
        if 'trip_distance' in data.columns:
            data['trip_distance'] = pd.to_numeric(data['trip_distance'], errors='coerce')
            print("\n📍 Top 5 viajes más largos:")
            print(data.nlargest(5, 'trip_distance')[['trip_distance', 'fare_amount', 'total_amount']])

        # Distribución por vendor
        if 'vendorid' in data.columns:
            print("\n🏢 Viajes por vendor:")
            print(data['vendorid'].value_counts())

        # Pagos por tipo
        if 'payment_type' in data.columns:
            print("\n💳 Distribución de tipos de pago:")
            print(data['payment_type'].value_counts())