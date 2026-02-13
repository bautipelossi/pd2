import pandas as pd
from pathlib import Path

# =====================================
# 📂 RUTAS
# =====================================

BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]

DATA_DIR = PROJECT_ROOT / "datos" / "limpios"

FHV_PATH = DATA_DIR / "fhv_2023_clean.parquet"
LTC_PATH = DATA_DIR / "nyc_taxi_clean.parquet"
WEATHER_PATH = DATA_DIR / "nyc_weather_2023_first_week.csv"

OUTPUT_PATH = DATA_DIR / "hourly_aggregate.parquet"

# ==========================================
# 1. CARGA DE DATOS
# ==========================================

COLUMNS_LTC = [
    'tpep_pickup_datetime',
    'trip_distance',
    'fare_amount',
    'total_amount',
    'trip_duration_min'
]

COLUMNS_FHV = [
    'pickup_datetime',
    'trip_miles',
    'base_passenger_fare',
    'tolls',
    'tips'
    'trip_duration_min'
]
def load_data():
    ltc = pd.read_parquet(LTC_PATH, columns = COLUMNS_LTC)
    fhv = pd.read_parquet(FHV_PATH)
    weather = pd.read_csv(WEATHER_PATH)

    return ltc, fhv, weather


# ==========================================
# 2. PREPARACIÓN TEMPORAL
# ==========================================

def prepare_datetime(ltc, fhv, weather):

    #TODO: Meter esto en una funcion a parte
    ltc.rename(columns={
        'tpep_pickup_datetime': 'pickup_datetime',
    }, inplace=True)

    fhv.rename(columns={
        'trip_miles': 'trip_distance',
        'base_passenger_fare': 'fare_amount'
    }, inplace=True)

    fhv['total_amount'] = fhv['fare_amount'] + fhv['tolls']

    #TODO: Lo de arriba separarlo de aqui
    ltc["pickup_datetime"] = pd.to_datetime(ltc["pickup_datetime"])
    fhv["pickup_datetime"] = pd.to_datetime(fhv["pickup_datetime"])
    weather.index = pd.to_datetime(weather.index)

    ltc["datetime_hour"] = ltc["pickup_datetime"].dt.floor("h")
    fhv["datetime_hour"] = fhv["pickup_datetime"].dt.floor("h")

    return ltc, fhv, weather


# ==========================================
# 3. AGREGACIÓN POR SERVICIO
# ==========================================

def aggregate_service(df, service_name):

    agg = (
        df.groupby("datetime_hour")
        .agg(
            trip_count=("pickup_datetime", "count"),
            avg_fare=("fare_amount", "mean"),
            avg_trip_distance=("trip_distance", "mean"),
            avg_trip_duration=("trip_duration_min", "mean"),
            total_revenue=("total_amount", "sum"),
        )
        .reset_index()
    )

    agg["service_type"] = service_name

    return agg


# ==========================================
# 4. VARIABLES TEMPORALES
# ==========================================

def add_time_features(df):

    df["hour"] = df["datetime_hour"].dt.hour
    df["weekday"] = df["datetime_hour"].dt.weekday
    df["month"] = df["datetime_hour"].dt.month
    df["is_weekend"] = df["weekday"].isin([5, 6])

    return df


# ==========================================
# 5. MERGE CON CLIMA
# ==========================================

def merge_weather(mobility_df, weather_df):


    weather_df = weather_df.reset_index().rename(
        columns={"date": "datetime_hour"}
    )

    weather_df['datetime_hour'] = pd.to_datetime(weather_df['datetime_hour'])

    if hasattr(weather_df['datetime_hour'].dt, 'tz'):
        weather_df['datetime_hour'] = weather_df['datetime_hour'].dt.tz_localize(None)

    merged = mobility_df.merge(
        weather_df,
        on="datetime_hour",
        how="left"
    )

    return merged


# ==========================================
# 6. GUARDAR DATASET FINAL
# ==========================================

def save_dataset(df):

    df.to_parquet(OUTPUT_PATH, index=False)
    print("Dataset agregado guardado correctamente.")


# ==========================================
# MAIN
# ==========================================

def main():

    ltc, fhv, weather = load_data()

    ltc, fhv, weather = prepare_datetime(ltc, fhv, weather)

    ltc_agg = aggregate_service(ltc, "taxi")
    fhv_agg = aggregate_service(fhv, "uber")

    mobility = pd.concat([ltc_agg, fhv_agg], ignore_index=True)

    mobility = add_time_features(mobility)

    mobility_weather = merge_weather(mobility, weather)

    save_dataset(mobility_weather)


if __name__ == "__main__":
    main()
