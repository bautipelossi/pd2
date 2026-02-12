import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import pyarrow.parquet as pq

# =====================================
# 📂 RUTAS
# =====================================

BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]

DATA_DIR = PROJECT_ROOT / "datos" / "limpios"

TAXI_PATH = DATA_DIR / "nyc_taxi_clean.parquet"
UBER_PATH = DATA_DIR / "fhv_2023_clean.parquet"
WEATHER_PATH = DATA_DIR / "nyc_weather_2023_first_week.csv"

#OUTPUT_PATH = DATA_DIR / "taxi_uber_weather_hourly.parquet"

sns.set_style("whitegrid")


# =====================================
# 📌 COLUMNAS A TENER EN CUENTA
# =====================================

taxi_cols = [
    'tpep_pickup_datetime',
    'trip_distance',
    'pulocationid',
    'congestion_surcharge',
    'total_amount',
    'trip_duration_min',
    'revenue_per_mile'
]

fhv_cols = [
    "pickup_datetime",
    "pulocationid",
    "trip_miles",
    "tips",
    "driver_pay"
]


# =====================================
# 🚕 AGREGACIÓN HORARIA
# =====================================

def aggregate_hourly(df, datetime_col, service_name):
    df["hour"] = df[datetime_col].dt.floor("H")

    hourly = (
        df
        .groupby("hour")
        .agg(
            trips=(datetime_col, "count"),
            avg_revenue=("total_amount", "mean"),
            revenue_per_mile=("revenue_per_mile", "mean")
        )
        .reset_index()
    )

    hourly["service"] = service_name
    return hourly


# =====================================
# 📊 VISUALIZACIONES
# =====================================

def plot_demand_over_time(df):
    plt.figure(figsize=(12,6))
    sns.lineplot(data=df, x="hour", y="trips", hue="service")
    plt.title("Demanda horaria Taxi vs Uber")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def plot_rain_impact(df):
    rainy = df[df["rain"] > 0]

    plt.figure(figsize=(8,6))
    sns.boxplot(data=rainy, x="service", y="trips")
    plt.title("Impacto de lluvia en número de viajes")
    plt.tight_layout()
    plt.show()


def plot_precipitation_sensitivity(df):
    sns.lmplot(
        data=df,
        x="precipitation",
        y="trips",
        hue="service",
        scatter_kws={"alpha":0.3},
        height=6,
        aspect=1.2
    )
    plt.title("Sensibilidad a la precipitación")
    plt.tight_layout()
    plt.show()


def plot_market_share(df):
    df["market_share"] = (
        df["trips"] /
        df.groupby("hour")["trips"].transform("sum")
    )

    plt.figure(figsize=(12,6))
    sns.lineplot(data=df, x="hour", y="market_share", hue="service")
    plt.title("Market Share por hora")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# =====================================
# 🔄 MAIN
# =====================================
def main():

    print("🚀 INICIANDO ANÁLISIS TAXI vs UBER vs WEATHER")

    # ---------------------
    # Cargar datos
    # ---------------------

    taxi = pd.read_parquet(TAXI_PATH, columns = taxi_cols)
    uber = pd.read_parquet(UBER_PATH, columns = fhv_cols)
    weather = pd.read_csv(WEATHER_PATH)

    print("Datos cargados correctamente")

    # ---------------------
    # Asegurar datetime
    # ---------------------

    taxi["tpep_pickup_datetime"] = pd.to_datetime(taxi["tpep_pickup_datetime"])
    uber["pickup_datetime"] = pd.to_datetime(uber["pickup_datetime"])

    # ---------------------
    # Agregación horaria
    # ---------------------

    taxi_hourly = aggregate_hourly(
        taxi,
        "tpep_pickup_datetime",
        "Taxi"
    )

    uber_hourly = aggregate_hourly(
        uber,
        "pickup_datetime",
        "Uber"
    )

    combined = pd.concat([taxi_hourly, uber_hourly], ignore_index=True)

    # ---------------------
    # Preparar weather
    # ---------------------

    weather = weather.reset_index().rename(columns={"date": "hour"})
    weather["hour"] = pd.to_datetime(weather["hour"])

    # ---------------------
    # Merge final
    # ---------------------

    combined = combined.merge(
        weather,
        on="hour",
        how="left"
    )

    print("Merge con weather completado")

    # ---------------------
    # Guardar dataset final
    # ---------------------

    #combined.to_parquet(
     #   OUTPUT_PATH,
      #  index=False,
       # engine="pyarrow",
        #compression="snappy"
    #)

    #print(f"Dataset final guardado en: {OUTPUT_PATH}")

    # ---------------------
    # Visualizaciones
    # ---------------------

    plot_demand_over_time(combined)
    plot_rain_impact(combined)
    plot_precipitation_sensitivity(combined)
    plot_market_share(combined)

    print("✅ Análisis completado")


if __name__ == "__main__":
    main()