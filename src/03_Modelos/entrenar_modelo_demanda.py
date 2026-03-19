import io
import logging
import os
import shutil
import zipfile
from pathlib import Path
from typing import Dict, Optional, Tuple

import branca.colormap as cm
import folium
import geopandas as gpd
import requests
from dotenv import load_dotenv
from folium import plugins

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# -----------------------------------------------------------------------------
# Configuración inicial (carga entorno). Incluye fallback por si falla conexión
# -----------------------------------------------------------------------------
load_dotenv()

MINIO_CONFIG: Dict[str, str] = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "https://minio.fdi.ucm.es"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "llcNNHgOBCdDA95Q1sma"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "jEtVGZry2V12u1VO22tYBqcUnua3U4W2s7NbOR2Z"),
    "path_style": os.getenv("MINIO_PATH_STYLE", "true"),
}

S3_PATHS = {
    "taxi": "s3a://pd2/taxomanos/limpios/nyc_taxi_clean.parquet",
    "fhv": "s3a://pd2/taxomanos/limpios/fhv_2023_clean.parquet",
    "restaurants": "s3a://pd2/taxomanos/limpios/restaurantes_nyc_clean.csv",
}

LOCAL_FALLBACKS = {
    "taxi": "datos/limpios/nyc_taxi_clean.parquet",
    "fhv": "datos/limpios/fhv_2023_clean.parquet",
    "restaurants": "datos/limpios/restaurantes_nyc_clean.csv",
}

TAXI_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

OUTPUT_DATA_DIR = Path("outputs")
OUTPUT_MAP_DIR = Path("src/Visualizacion/Cluster_adquisitivo")
OUTPUT_HTML = OUTPUT_MAP_DIR / "mapa_poder_adquisitivo.html"
OUTPUT_PARQUET = OUTPUT_DATA_DIR / "zonas_aggregated.parquet"
PROJECT_ROOT = Path(__file__).resolve().parents[2]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("cluster_zonas")


# -----------------------------------------------------------------------------
# Spark session
# -----------------------------------------------------------------------------
def crear_spark_session() -> SparkSession:
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]

    spark = (
        SparkSession.builder.appName("NYC_Taxi_FHV_Poder_Adquisitivo")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", MINIO_CONFIG["path_style"])
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        # Timeouts en milisegundos para evitar parseos ambiguos en Hadoop/S3A.
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.ttl", "600000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.hadoop.fs.s3a.multipart.purge", "false")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config("spark.hadoop.fs.s3a.retry.limit", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark iniciado: %s", spark.version)
    return spark


# -----------------------------------------------------------------------------
# I/O helpers
# -----------------------------------------------------------------------------
def preferir_local() -> bool:
    # En Windows priorizamos local para evitar bloqueos de lectura S3A en entornos docentes.
    mode = os.getenv("PREFER_LOCAL_DATA", "auto").strip().lower()
    if mode in {"1", "true", "yes", "local"}:
        return True
    if mode in {"0", "false", "no", "s3"}:
        return False
    return os.name == "nt"


def resolve_project_path(path_str: str) -> Path:
    p = Path(path_str)
    if p.is_absolute():
        return p
    return (PROJECT_ROOT / p).resolve()


def cargar_parquet_con_fallback(spark: SparkSession, key: str) -> DataFrame:
    s3_path = S3_PATHS[key]
    local_path = LOCAL_FALLBACKS.get(key)
    local_abs = resolve_project_path(local_path) if local_path else None

    if preferir_local():
        if local_abs and local_abs.exists():
            logger.info("Leyendo %s desde local (prioridad Windows): %s", key, local_abs)
            return spark.read.parquet(str(local_abs))
        raise FileNotFoundError(
            f"No existe fallback local para {key}: {local_abs}. "
            "En Windows se evita MinIO por defecto para no bloquear la sesion. "
            "Si quieres forzar S3, define PREFER_LOCAL_DATA=s3"
        )

    try:
        logger.info("Leyendo %s desde MinIO: %s", key, s3_path)
        return spark.read.parquet(s3_path)
    except Exception as s3_err:
        logger.warning("Fallo MinIO para %s: %s", key, str(s3_err).splitlines()[0])

    if local_abs and local_abs.exists():
        logger.info("Intentando fallback local: %s", local_abs)
        return spark.read.parquet(str(local_abs))

    raise RuntimeError(f"No se pudo leer dataset {key} desde S3 ni local")


def find_first_existing_column(df: DataFrame, candidates) -> Optional[str]:
    cols_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower() in cols_lower:
            return cols_lower[c.lower()]
    return None


# -----------------------------------------------------------------------------
# Metricas ETL
# -----------------------------------------------------------------------------
def preparar_taxi(df_taxi_raw: DataFrame) -> DataFrame:
    pickup_col = find_first_existing_column(
        df_taxi_raw,
        ["PULocationID", "pulocationid", "pickup_location_id"],
    )
    if not pickup_col:
        raise ValueError("Taxi no tiene columna de pickup_location_id/PULocationID")

    tip_col = find_first_existing_column(df_taxi_raw, ["tip_amount"])
    passenger_col = find_first_existing_column(df_taxi_raw, ["passenger_count"])

    if not tip_col or not passenger_col:
        raise ValueError("Taxi no tiene tip_amount o passenger_count")

    df = (
        df_taxi_raw.select(
            F.col(pickup_col).alias("LocationID"),
            F.col(tip_col).cast(DoubleType()).alias("tip_amount"),
            F.col(passenger_col).cast(DoubleType()).alias("passenger_count"),
        )
        .filter(F.col("LocationID").isNotNull())
        .filter((F.col("LocationID") > 0) & (F.col("LocationID") <= 265))
    )

    taxi_agg = df.groupBy("LocationID").agg(
        F.avg("tip_amount").alias("tip_amount_avg"),
        F.avg("passenger_count").alias("passenger_count_avg"),
        F.count("*").alias("taxi_trip_count"),
    )
    return taxi_agg


def preparar_fhv(df_fhv_raw: DataFrame) -> DataFrame:
    pickup_col = find_first_existing_column(
        df_fhv_raw,
        ["PULocationID", "pulocationid", "pickup_location_id"],
    )
    if not pickup_col:
        raise ValueError("FHV no tiene columna de pickup_location_id/PULocationID")

    df = (
        df_fhv_raw.select(F.col(pickup_col).alias("LocationID"))
        .filter(F.col("LocationID").isNotNull())
        .filter((F.col("LocationID") > 0) & (F.col("LocationID") <= 265))
    )

    fhv_agg = df.groupBy("LocationID").agg(F.count("*").alias("fhv_trip_count"))
    return fhv_agg


def combinar_metricas(taxi_agg: DataFrame, fhv_agg: DataFrame) -> DataFrame:
    df = taxi_agg.join(fhv_agg, on="LocationID", how="full_outer").fillna(
        {
            "tip_amount_avg": 0.0,
            "passenger_count_avg": 0.0,
            "taxi_trip_count": 0,
            "fhv_trip_count": 0,
        }
    )

    df = df.withColumn(
        "volumen_viajes",
        F.col("taxi_trip_count") + F.col("fhv_trip_count"),
    )
    return df


def calcular_indice_poder_adquisitivo(df: DataFrame) -> DataFrame:
    assembler = VectorAssembler(
        inputCols=["tip_amount_avg", "passenger_count_avg", "volumen_viajes"],
        outputCol="features_raw",
    )
    df_vec = assembler.transform(df)

    scaler = MinMaxScaler(inputCol="features_raw", outputCol="features_scaled")
    scaler_model = scaler.fit(df_vec)
    df_scaled = scaler_model.transform(df_vec)

    arr_col = vector_to_array("features_scaled")
    df_scaled = (
        df_scaled.withColumn("tip_norm", arr_col[0])
        .withColumn("passenger_norm", arr_col[1])
        .withColumn("volumen_norm", arr_col[2])
    )

    df_idx = df_scaled.withColumn(
        "poder_adquisitivo",
        F.col("tip_norm") * F.lit(0.5)
        + F.col("passenger_norm") * F.lit(0.2)
        + F.col("volumen_norm") * F.lit(0.3),
    )

    df_idx = df_idx.withColumn(
        "poder_adquisitivo_0_100", F.col("poder_adquisitivo") * F.lit(100.0)
    )

    return df_idx.drop("features_raw", "features_scaled")


def aplicar_kmeans(df: DataFrame, k: int = 5) -> Tuple[DataFrame, KMeans]:
    assembler = VectorAssembler(
        inputCols=["tip_norm", "passenger_norm", "volumen_norm"],
        outputCol="kmeans_features",
    )
    df_vec = assembler.transform(df)

    kmeans = KMeans(
        k=k,
        seed=42,
        featuresCol="kmeans_features",
        predictionCol="cluster",
        maxIter=100,
    )
    model = kmeans.fit(df_vec)
    pred = model.transform(df_vec).drop("kmeans_features")
    return pred, model


# -----------------------------------------------------------------------------
# Geo + mapa
# -----------------------------------------------------------------------------
def descargar_y_encontrar_shapefile(base_dir: Path = Path("taxi_zones")) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)

    shp_files = list(base_dir.rglob("*.shp"))
    if shp_files:
        best = sorted(shp_files, key=lambda p: (p.name != "taxi_zones.shp", len(str(p))))[0]
        logger.info("Shapefile encontrado en cache: %s", best)
        return best

    logger.info("Descargando shapefile desde: %s", TAXI_ZONES_URL)
    resp = requests.get(TAXI_ZONES_URL, timeout=60)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(base_dir)

    shp_files = list(base_dir.rglob("*.shp"))
    if not shp_files:
        raise FileNotFoundError("No se encontro .shp tras extraer taxi_zones.zip")

    best = sorted(shp_files, key=lambda p: (p.name != "taxi_zones.shp", len(str(p))))[0]
    logger.info("Shapefile detectado: %s", best)
    return best


def cargar_zonas_gdf() -> gpd.GeoDataFrame:
    shp_path = descargar_y_encontrar_shapefile()
    gdf = gpd.read_file(shp_path)

    if "LocationID" not in gdf.columns:
        for c in ["OBJECTID", "objectid", "locationid", "LocationId"]:
            if c in gdf.columns:
                gdf = gdf.rename(columns={c: "LocationID"})
                break

    if "LocationID" not in gdf.columns:
        raise ValueError("El shapefile no contiene LocationID/OBJECTID")

    gdf["LocationID"] = gdf["LocationID"].astype(int)

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    return gdf


def cargar_restaurantes_filtrados(spark: SparkSession, max_points: int = 1500) -> Optional[gpd.GeoDataFrame]:
    try:
        source = S3_PATHS["restaurants"]
        local_rest = LOCAL_FALLBACKS.get("restaurants")
        local_rest_abs = resolve_project_path(local_rest) if local_rest else None

        if preferir_local():
            if local_rest_abs and local_rest_abs.exists():
                source = str(local_rest_abs)
            else:
                logger.info("No hay CSV local de restaurantes; se omite capa de restaurantes.")
                return None

        df = spark.read.option("header", True).option("inferSchema", True).csv(source)

        rating_col = find_first_existing_column(df, ["rating", "stars", "score"])
        lat_col = find_first_existing_column(df, ["latitude", "lat", "y"])
        lon_col = find_first_existing_column(df, ["longitude", "lon", "lng", "x"])

        if not rating_col or not lat_col or not lon_col:
            logger.warning("CSV restaurantes sin columnas esperadas de rating/lat/lon.")
            return None

        df_f = (
            df.filter(F.col(rating_col) > 4)
            .filter(F.col(lat_col).isNotNull() & F.col(lon_col).isNotNull())
            .select(
                F.col(rating_col).cast(DoubleType()).alias("rating"),
                F.col(lat_col).cast(DoubleType()).alias("lat"),
                F.col(lon_col).cast(DoubleType()).alias("lon"),
                *[F.col(c) for c in df.columns if c.lower() in ("name", "restaurant", "nombre")]
            )
            .limit(max_points)
        )

        pdf = df_f.toPandas()
        if pdf.empty:
            return None

        gdf = gpd.GeoDataFrame(
            pdf,
            geometry=gpd.points_from_xy(pdf["lon"], pdf["lat"]),
            crs="EPSG:4326",
        )
        return gdf
    except Exception as e:
        logger.warning("No se pudo cargar restaurantes: %s", str(e).splitlines()[0])
        return None


def crear_mapa(gdf_merged: gpd.GeoDataFrame, usar_cluster: bool = True) -> folium.Map:
    nyc_center = [40.7128, -74.0060]
    m = folium.Map(location=nyc_center, zoom_start=10, tiles="cartodbpositron", control_scale=True)

    min_val = float(gdf_merged["poder_adquisitivo_0_100"].min())
    max_val = float(gdf_merged["poder_adquisitivo_0_100"].max())

    colormap = cm.LinearColormap(
        colors=["#f7fbff", "#6baed6", "#2171b5", "#08306b"],
        vmin=min_val,
        vmax=max_val,
        caption="Indice de poder adquisitivo (0-100)",
    )

    def style_power(feature):
        value = feature["properties"].get("poder_adquisitivo_0_100", 0.0)
        fill = colormap(value) if value is not None else "#d9d9d9"
        return {
            "fillColor": fill,
            "color": "#333333",
            "weight": 0.6,
            "fillOpacity": 0.75,
        }

    tooltip_fields = [
        "zone",
        "borough",
        "LocationID",
        "tip_amount_avg",
        "passenger_count_avg",
        "taxi_trip_count",
        "fhv_trip_count",
        "volumen_viajes",
        "poder_adquisitivo_0_100",
    ]
    tooltip_fields = [c for c in tooltip_fields if c in gdf_merged.columns]
    aliases = [
        "Zone",
        "Borough",
        "LocationID",
        "Tip promedio",
        "Pasajeros promedio",
        "Viajes taxi",
        "Viajes FHV",
        "Volumen total",
        "Poder adquisitivo",
    ][: len(tooltip_fields)]

    layer_power = folium.GeoJson(
        gdf_merged,
        name="Poder adquisitivo",
        style_function=style_power,
        highlight_function=lambda _: {"weight": 2, "fillOpacity": 0.95},
        tooltip=folium.GeoJsonTooltip(fields=tooltip_fields, aliases=aliases, sticky=True),
    )
    layer_power.add_to(m)
    colormap.add_to(m)

    if usar_cluster and "cluster" in gdf_merged.columns:
        cluster_colors = {
            0: "#e41a1c", 1: "#377eb8", 2: "#4daf4a", 3: "#984ea3", 4: "#ff7f00", 5: "#a65628", 6: "#f781bf", 7: "#999999",
        }

        def style_cluster(feature):
            c = feature["properties"].get("cluster", -1)
            color = cluster_colors.get(int(c), "#666666")
            return {
                "fillColor": color,
                "color": "#222222",
                "weight": 0.6,
                "fillOpacity": 0.55,
            }

        layer_cluster = folium.GeoJson(
            gdf_merged,
            name="Cluster KMeans",
            style_function=style_cluster,
            highlight_function=lambda _: {"weight": 2, "fillOpacity": 0.9},
            tooltip=folium.GeoJsonTooltip(
                fields=[c for c in ["zone", "borough", "LocationID", "cluster"] if c in gdf_merged.columns],
                aliases=["Zone", "Borough", "LocationID", "Cluster"],
                sticky=True,
            ),
            show=False,
        )
        layer_cluster.add_to(m)

    plugins.MiniMap(toggle_display=True).add_to(m)
    plugins.Fullscreen(position="topleft").add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)

    return m


def agregar_capa_restaurantes(m: folium.Map, gdf_rest: Optional[gpd.GeoDataFrame]) -> folium.Map:
    if gdf_rest is None or gdf_rest.empty:
        return m

    fg = folium.FeatureGroup(name="Restaurantes rating > 4", show=False)
    for _, row in gdf_rest.iterrows():
        popup_parts = [f"Rating: {row.get('rating', 'N/A')}"]
        if "name" in row and row["name"] is not None:
            popup_parts.insert(0, f"Nombre: {row['name']}")
        elif "restaurant" in row and row["restaurant"] is not None:
            popup_parts.insert(0, f"Restaurante: {row['restaurant']}")
        elif "nombre" in row and row["nombre"] is not None:
            popup_parts.insert(0, f"Nombre: {row['nombre']}")

        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=3,
            color="#2ca25f",
            fill=True,
            fillColor="#2ca25f",
            fillOpacity=0.65,
            popup="<br>".join(popup_parts),
        ).add_to(fg)

    fg.add_to(m)
    return m


# -----------------------------------------------------------------------------
# Save outputs
# -----------------------------------------------------------------------------
def guardar_outputs(df_final: DataFrame, mapa: folium.Map) -> None:
    output_data_dir_abs = resolve_project_path(str(OUTPUT_DATA_DIR))
    output_map_dir_abs = resolve_project_path(str(OUTPUT_MAP_DIR))
    output_parquet_abs = resolve_project_path(str(OUTPUT_PARQUET))
    output_html_abs = resolve_project_path(str(OUTPUT_HTML))

    output_data_dir_abs.mkdir(parents=True, exist_ok=True)
    output_map_dir_abs.mkdir(parents=True, exist_ok=True)

    # Si existe como carpeta (resto de una escritura Spark anterior), la borramos.
    if output_parquet_abs.exists():
        if output_parquet_abs.is_dir():
            shutil.rmtree(output_parquet_abs)
        else:
            output_parquet_abs.unlink()

    # Guardamos usando Pandas para evitar el error de "NativeIO" en Windows
  
    pdf_final = df_final.toPandas()
    tmp_parquet = output_parquet_abs.with_suffix(".tmp.parquet")
    if tmp_parquet.exists():
        tmp_parquet.unlink()

    pdf_final.to_parquet(str(tmp_parquet), index=False, engine="pyarrow")
    tmp_parquet.replace(output_parquet_abs)
    logger.info("Dataset agregado guardado localmente en: %s", output_parquet_abs)

    mapa.save(str(output_html_abs))
    logger.info("Mapa interactivo guardado en: %s", output_html_abs)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    spark = None
    try:
        logger.info("=== Inicio pipeline NYC Taxi + FHV ===")
        spark = crear_spark_session()

        taxi_raw = cargar_parquet_con_fallback(spark, "taxi")
        fhv_raw = cargar_parquet_con_fallback(spark, "fhv")

        taxi_agg = preparar_taxi(taxi_raw)
        fhv_agg = preparar_fhv(fhv_raw)
        metricas = combinar_metricas(taxi_agg, fhv_agg)
        metricas = calcular_indice_poder_adquisitivo(metricas)

        # Bonus clustering
        metricas, model = aplicar_kmeans(metricas, k=5)
        logger.info("KMeans entrenado. K=%s", model.getK())

        # Solo al final pasamos a pandas/geopandas para mapa
        pdf = metricas.toPandas()
        gdf_metricas = gpd.GeoDataFrame(pdf)

        gdf_zonas = cargar_zonas_gdf()
        gdf_merged = gdf_zonas.merge(gdf_metricas, on="LocationID", how="left")

        # Fill para zonas sin viajes
        fill_cols = [
            "tip_amount_avg", "passenger_count_avg", "taxi_trip_count", "fhv_trip_count", 
            "volumen_viajes", "tip_norm", "passenger_norm", "volumen_norm", 
            "poder_adquisitivo", "poder_adquisitivo_0_100", "cluster"
        ]
        for c in fill_cols:
            if c in gdf_merged.columns:
                gdf_merged[c] = gdf_merged[c].fillna(0)

        m = crear_mapa(gdf_merged, usar_cluster=True)

        # Capa extra opcional restaurantes
        gdf_rest = cargar_restaurantes_filtrados(spark)
        m = agregar_capa_restaurantes(m, gdf_rest)

        # Guardar parquet final y mapa HTML
        guardar_outputs(metricas, m)

        logger.info("=== Pipeline finalizado OK ===")
    except Exception as e:
        logger.exception("Error en el pipeline: %s", e)
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("Spark detenido.")
            except Exception as stop_err:
                logger.warning("No se pudo detener Spark limpiamente: %s", stop_err)


if __name__ == "__main__":
    main()