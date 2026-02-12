import pandas as pd
from pathlib import Path

#En este archivo se unirán los datasets de FHV (2023) y YLC (2023)
# =====================================================
# RUTAS
# =====================================================

BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]

FHV_PATH = PROJECT_ROOT / "datos" / "procesados" / "fhv_2023_clean.parquet"
YLC_PATH = PROJECT_ROOT / "datos" / "procesados" / "nyc_taxi_clean.parquet"

OUTPUT_PATH = PROJECT_ROOT / "datos" / "procesados" / "resumen_zona.parquet"


# =====================================================
# FUNCIONES
# =====================================================

def cargar_y_unir():
    df_fhv = pd.read_parquet(FHV_PATH)
    df_ylc = pd.read_parquet(YLC_PATH)

    df_fhv = df_fhv.rename(columns={"trip_miles": "trip_distance"})
    df_ylc = df_ylc.rename(columns={
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime"
    })

    df_fhv["tipo_servicio"] = "FHV"
    df_ylc["tipo_servicio"] = "YLC"

    columnas = ["pulocationid", "tipo_servicio"]

    df_total = pd.concat(
        [df_fhv[columnas], df_ylc[columnas]],
        ignore_index=True
    )

    return df_total


def agregar_por_zona(df_total):

    agg = (
        df_total
        .groupby(["pulocationid", "tipo_servicio"])
        .size()
        .reset_index(name="viajes")
    )

    pivot = (
        agg
        .pivot(index="pulocationid", columns="tipo_servicio", values="viajes")
        .fillna(0)
        .reset_index()
        .rename(columns={"pulocationid": "LocationID"})
    )

    if "FHV" not in pivot.columns:
        pivot["FHV"] = 0
    if "YLC" not in pivot.columns:
        pivot["YLC"] = 0

    pivot["total"] = pivot["FHV"] + pivot["YLC"]


    # Market share FHV
    pivot["market_share_fhv"] = pivot["FHV"] / (pivot["FHV"] + pivot["YLC"] + 1)

    # Diferencia absoluta
    pivot["diff_fhv_minus_ylc"] = pivot["FHV"] - pivot["YLC"]


    return pivot


def main():
    print("Cargando datos pesados...")
    df_total = cargar_y_unir()

    print("Agregando por zona...")
    resumen = agregar_por_zona(df_total)

    resumen.to_parquet(OUTPUT_PATH)
    print("Resumen guardado en:", OUTPUT_PATH)


if __name__ == "__main__":
    main()
