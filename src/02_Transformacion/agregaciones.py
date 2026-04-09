import pandas as pd
import numpy as np
from pathlib import Path

"""
    En este Script se hace un único dataset en el que se combinan los datos de 
    los taxis con los de Uber, dando un extra de información sobre el volumen
    de viajes, el ratio o el tamaño del mercado.
    
    Columnas del dataset nuevo (Preparado para Spark ML):
        - "pulocationid" : ID de la localización donde comienza el viaje (int64)
        - "day_of_week" : Día de la semana formato Spark (1=Dom, 2=Lun... 7=Sab) (int64)
        - "pickup_hour" : Hora de comienzo del viaje (0-23) (int64)
        - "FHV" : Volumen absoluto de viajes FHV en esa hora (int64)
        - "YLC" : Volumen absoluto de viajes en taxi en esa hora (int64)
        - "demanda_viajes" : Tamaño del mercado, FHV + YLC. TARGET DEL MODELO (int64)
        - "market_share" : Porcentaje de viajes en FHV sobre el total (float64)
        - "ratio" : Ventaja relativa FHV vs YLC (float64)
"""

# =====================================================
# RUTAS AJUSTADAS A LA NUEVA ESTRUCTURA DEL REPO
# =====================================================
# __file__ está en: \pd2\src\02_Transformacion\agregaciones.py
BASE_DIR = Path(__file__).resolve()

# parents[2] sube de agregaciones.py -> 02_Transformacion -> src -> pd2
PROJECT_ROOT = BASE_DIR.parents[2]

# Carpeta de datos
DATA_DIR = PROJECT_ROOT / "Entrega1_Pd2" / "datos" / "limpios"

FHV_PATH = DATA_DIR / "fhv_2023_clean.parquet"
YLC_PATH = DATA_DIR / "nyc_taxi_clean.parquet"

OUTPUT_PATH = DATA_DIR / "resumen_zona_hora.parquet"


# =====================================================
# CARGA + NORMALIZACIÓN
# =====================================================
def cargar_y_normalizar():
    print(" Leyendo parquets...")
    df_fhv = pd.read_parquet(FHV_PATH)
    df_ylc = pd.read_parquet(YLC_PATH)

    # --- Normalizar nombres de columnas para tener un esquema común ---
    if "pickup_datetime" not in df_fhv.columns:
        raise RuntimeError(f"FHV no tiene 'pickup_datetime'. Columnas: {list(df_fhv.columns)}")
    if "pulocationid" not in df_fhv.columns:
        raise RuntimeError(f"FHV no tiene 'pulocationid'. Columnas: {list(df_fhv.columns)}")

    if "pickup_datetime" not in df_ylc.columns:
        if "tpep_pickup_datetime" in df_ylc.columns:
            df_ylc = df_ylc.rename(columns={"tpep_pickup_datetime": "pickup_datetime"})
        else:
            raise RuntimeError(
                "YLC no tiene 'pickup_datetime' ni 'tpep_pickup_datetime'. "
                f"Columnas: {list(df_ylc.columns)}"
            )
    if "pulocationid" not in df_ylc.columns:
        raise RuntimeError(f"YLC no tiene 'pulocationid'. Columnas: {list(df_ylc.columns)}")

    # --- Asegurar datetime ---
    df_fhv["pickup_datetime"] = pd.to_datetime(df_fhv["pickup_datetime"], errors="coerce")
    df_ylc["pickup_datetime"] = pd.to_datetime(df_ylc["pickup_datetime"], errors="coerce")

    # --- Filtrar nulos críticos ---
    df_fhv = df_fhv.dropna(subset=["pickup_datetime", "pulocationid"])
    df_ylc = df_ylc.dropna(subset=["pickup_datetime", "pulocationid"])

    # --- Tipo de servicio ---
    df_fhv["tipo_servicio"] = "FHV"
    df_ylc["tipo_servicio"] = "YLC"

    # --- Columnas mínimas ---
    df_fhv = df_fhv[["pickup_datetime", "pulocationid", "tipo_servicio"]]
    df_ylc = df_ylc[["pickup_datetime", "pulocationid", "tipo_servicio"]]

    df_total = pd.concat([df_fhv, df_ylc], ignore_index=True)

    # --- Hora (0-23) ---
    df_total["pickup_hour"] = df_total["pickup_datetime"].dt.hour.astype(int)

    # --- Día de la semana (Adaptado a formato Spark: 1=Domingo, 2=Lunes... 7=Sábado) ---
    df_total["day_of_week"] = ((df_total["pickup_datetime"].dt.dayofweek + 1) % 7) + 1

    # Normalizar ID a int
    df_total["pulocationid"] = df_total["pulocationid"].astype(int)

    return df_total


# =====================================================
# AGREGACIÓN ZONA + HORA + DÍA DE LA SEMANA
# =====================================================
def agregar_por_zona_hora(df_total: pd.DataFrame) -> pd.DataFrame:
    print(" Agregando por (pulocationid, day_of_week, pickup_hour, tipo_servicio)...")

    agg = (
        df_total
        .groupby(["pulocationid", "day_of_week", "pickup_hour", "tipo_servicio"])
        .size()
        .reset_index(name="viajes")
    )

    pivot = (
        agg
        .pivot(index=["pulocationid", "day_of_week", "pickup_hour"], columns="tipo_servicio", values="viajes")
        .fillna(0)
        .reset_index()
    )

    # Asegurar columnas aunque falte alguna categoría por filtrado
    if "FHV" not in pivot.columns:
        pivot["FHV"] = 0
    if "YLC" not in pivot.columns:
        pivot["YLC"] = 0

    pivot["FHV"] = pivot["FHV"].astype(int)
    pivot["YLC"] = pivot["YLC"].astype(int)

    # Nueva columna target para el modelo
    pivot["demanda_viajes"] = pivot["FHV"] + pivot["YLC"]

    # Market share FHV (0..1)
    denom = pivot["demanda_viajes"].to_numpy()
    pivot["market_share"] = np.where(denom > 0, pivot["FHV"] / denom, 0.0).round(6)

    # Ratio FHV/YLC (evitar div por 0)
    pivot["ratio"] = (pivot["FHV"] / (pivot["YLC"] + 1)).round(6)

    # Orden prolijo
    pivot = pivot.sort_values(["day_of_week", "pickup_hour", "pulocationid"]).reset_index(drop=True)

    return pivot


def main():
    print(" Generando resumen_zona_hora.parquet")
    df_total = cargar_y_normalizar()
    resumen = agregar_por_zona_hora(df_total)

    # Crear carpeta de salida si no existe para evitar errores
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f" Guardando parquet en: {OUTPUT_PATH}")
    resumen.to_parquet(OUTPUT_PATH, index=False)
    print(" Listo.")


if __name__ == "__main__":
    main()