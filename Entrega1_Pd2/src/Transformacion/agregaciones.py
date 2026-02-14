import pandas as pd
import numpy as np
from pathlib import Path

# =====================================================
# RUTAS 
# =====================================================
BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]

FHV_PATH = PROJECT_ROOT / "datos" / "procesados" / "fhv_2023_clean.parquet"
YLC_PATH = PROJECT_ROOT / "datos" / "procesados" / "nyc_taxi_clean.parquet"

OUTPUT_PATH = PROJECT_ROOT / "datos" / "procesados" / "resumen_zona_hora.parquet"


# =====================================================
# CARGA + NORMALIZACIÓN
# =====================================================
def cargar_y_normalizar():
    print("📦 Leyendo parquets...")
    df_fhv = pd.read_parquet(FHV_PATH)
    df_ylc = pd.read_parquet(YLC_PATH)

    # --- Normalizar nombres de columnas para tener un esquema común ---
    # FHV: normalmente trae pickup_datetime + pulocationid
    if "pickup_datetime" not in df_fhv.columns:
        raise RuntimeError(f"FHV no tiene 'pickup_datetime'. Columnas: {list(df_fhv.columns)}")
    if "pulocationid" not in df_fhv.columns:
        raise RuntimeError(f"FHV no tiene 'pulocationid'. Columnas: {list(df_fhv.columns)}")

    # YLC: normalmente trae tpep_pickup_datetime + pulocationid
    # En tu agregaciones.py lo renombrabas a pickup_datetime
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

    # Normalizar ID a int (luego lo renombramos a LocationID)
    df_total["pulocationid"] = df_total["pulocationid"].astype(int)

    return df_total


# =====================================================
# AGREGACIÓN ZONA + HORA
# =====================================================
def agregar_por_zona_hora(df_total: pd.DataFrame) -> pd.DataFrame:
    print(" Agregando por (pulocationid, pickup_hour, tipo_servicio)...")

    agg = (
        df_total
        .groupby(["pulocationid", "pickup_hour", "tipo_servicio"])
        .size()
        .reset_index(name="viajes")
    )

    pivot = (
        agg
        .pivot(index=["pulocationid", "pickup_hour"], columns="tipo_servicio", values="viajes")
        .fillna(0)
        .reset_index()
        .rename(columns={"pulocationid": "LocationID"})
    )

    # Asegurar columnas aunque falte alguna categoría por filtrado
    if "FHV" not in pivot.columns:
        pivot["FHV"] = 0
    if "YLC" not in pivot.columns:
        pivot["YLC"] = 0

    pivot["FHV"] = pivot["FHV"].astype(int)
    pivot["YLC"] = pivot["YLC"].astype(int)

    pivot["total"] = pivot["FHV"] + pivot["YLC"]

    # Market share FHV (0..1)
    denom = pivot["total"].to_numpy()
    pivot["market_share"] = np.where(denom > 0, pivot["FHV"] / denom, 0.0).round(6)

    # Ratio FHV/YLC (evitar div por 0)
    pivot["ratio"] = (pivot["FHV"] / (pivot["YLC"] + 1)).round(6)

    # Orden prolijo
    pivot = pivot.sort_values(["pickup_hour", "LocationID"]).reset_index(drop=True)

    return pivot


def main():
    print(" Generando resumen_zona_hora.parquet")
    df_total = cargar_y_normalizar()
    resumen = agregar_por_zona_hora(df_total)

    print(f" Guardando parquet en: {OUTPUT_PATH}")
    resumen.to_parquet(OUTPUT_PATH, index=False)
    print(" Listo.")


if __name__ == "__main__":
    main()
