"""
================================================================================
MAPA COROPLÉTICO DE PODER ADQUISITIVO POR ZONA - NYC TAXI DATA 2023
================================================================================

Este script genera un mapa coroplético de Nueva York que representa el poder
adquisitivo estimado por zona usando datos de taxis y VTC (FHV).

DEPENDENCIAS:
    pip install pandas geopandas folium branca requests scipy

EJECUCIÓN:
    python mapa_poder_adquisitivo.py

================================================================================
"""

# ==============================================================================
# PASO 1: IMPORTAR LIBRERÍAS
# ==============================================================================

import os
import warnings
from typing import Tuple

import pandas as pd
import geopandas as gpd
import folium
from folium import plugins
import branca.colormap as cm
from scipy import stats
import requests
import zipfile
import io

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'
# Configuración de PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

warnings.filterwarnings('ignore')

# ==============================================================================
# PASO 2: CONFIGURACIÓN DE CREDENCIALES MINIO / S3
# ==============================================================================

# Credenciales MinIO
MINIO_CONFIG = {
    "endpoint": "https://minio.fdi.ucm.es",
    "access_key": "llcNNHgOBCdDA95Q1sma",
    "secret_key": "jEtVGZry2V12u1VO22tYBqcUnua3U4W2s7NbOR2Z",
    "path_style": "true"
}

# Rutas de datos en el bucket
S3_PATHS = {
    "taxi": "s3a://tu_bucket/datos/limpios/nyc_taxi_clean.parquet",
    "fhv": "s3a://tu_bucket/datos/limpios/fhv_2023_clean.parquet"
}

# Rutas Locales (Backup)
LOCAL_PATHS = {
    "taxi": r"C:\Users\Bauti\pd2\Entrega1_Pd2\datos\limpios\nyc_taxi_clean.parquet",
    "fhv": r"C:\Users\Bauti\pd2\Entrega1_Pd2\datos\limpios\fhv_2023_clean.parquet"
}
# URL del shapefile oficial de zonas de taxi de NYC
TAXI_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

# Archivo de salida
OUTPUT_FILE = "mapa_poder_adquisitivo_nyc.html"


# ==============================================================================
# PASO 3: CREAR Y CONFIGURAR SESIÓN SPARK PARA MINIO
# ==============================================================================

def crear_spark_session() -> SparkSession:
    """
    Crea y configura una sesión de Spark para conectarse a MinIO usando S3A.
    
    La configuración incluye:
    - Credenciales de acceso a MinIO
    - Endpoint personalizado
    - Path style access (necesario para MinIO)
    - Paquetes necesarios para S3A y AWS
    
    Returns:
        SparkSession: Sesión de Spark configurada
    """
    print("=" * 60)
    print("PASO 3: Configurando sesión de Spark para MinIO...")
    print("=" * 60)
    
    # Paquetes necesarios para conectividad S3
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    
    spark = (SparkSession.builder
        .appName("NYC_Taxi_Poder_Adquisitivo")
        .master("local[*]")  # Usar todos los cores disponibles
        
        # Configuración de memoria
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        
        # Paquetes Maven para S3
        .config("spark.jars.packages", ",".join(packages))
        
        # Configuración S3A para MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", MINIO_CONFIG["path_style"])
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # Deshabilitar SSL verification si es necesario (desarrollo)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        
        # Configuración adicional para compatibilidad
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        .getOrCreate()
    )
    
    # Reducir verbosidad de logs
    spark.sparkContext.setLogLevel("WARN")
    
    print(f" Spark Session creada: {spark.version}")
    print(f" Endpoint MinIO: {MINIO_CONFIG['endpoint']}")
    
    return spark


# ==============================================================================
# PASO 4: CARGAR DATOS (INTENTO MINIO -> BACKUP LOCAL)
# ==============================================================================

def cargar_dataset(spark: SparkSession, nombre_clave: str):
    """
    Intenta cargar datos desde MinIO. Si falla, carga desde el backup local.
    """
    print(f"\n--- Cargando dataset: {nombre_clave} ---")
    
    # 1. Intento con MinIO
    try:
        print(f"Intentando cargar desde MinIO: {S3_PATHS[nombre_clave]}...")
        df = spark.read.parquet(S3_PATHS[nombre_clave])
        print(f" {nombre_clave.capitalize()} cargado exitosamente desde MinIO.")
        return df
    except Exception as e:
        print(f" Fallo en MinIO: {str(e).splitlines()[0]}") # Solo mostramos la primera línea del error
        
    # 2. Intento con Local (Backup)
    try:
        print(f"Recurriendo al backup local: {LOCAL_PATHS[nombre_clave]}...")
        df = spark.read.parquet(LOCAL_PATHS[nombre_clave])
        print(f" {nombre_clave.capitalize()} cargado desde disco local.")
        return df
    except Exception as e:
        print(f" Error crítico: No se encontró el dataset en MinIO ni en Local.")
        raise e

def cargar_datos_taxi(spark: SparkSession):
    df_taxi = cargar_dataset(spark, "taxi")
    
    # --- NORMALIZACIÓN DE COLUMNAS ---
    # Si viene en minúsculas (pulocationid), la renombramos a CamelCase (PULocationID)
    if "pulocationid" in df_taxi.columns:
        df_taxi = df_taxi.withColumnRenamed("pulocationid", "PULocationID")
    
    columnas_taxi = ["PULocationID", "tip_amount", "passenger_count", "total_amount"]
    # Verificamos qué columnas existen realmente (en el log dice que tienes tip_amount, total_amount, etc.)
    columnas_existentes = [col for col in columnas_taxi if col in df_taxi.columns]
    
    df_taxi = df_taxi.select(columnas_existentes)
    df_taxi = df_taxi.withColumn("tipo_servicio", F.lit("taxi"))
    
    print(f" Datos de taxi normalizados y cargados.")
    return df_taxi

def cargar_datos_fhv(spark: SparkSession):
    df_fhv = cargar_dataset(spark, "fhv")
    
    if "PULocationID" in df_fhv.columns:
        df_fhv = df_fhv.select("PULocationID")
    elif "pickup_location_id" in df_fhv.columns:
        df_fhv = df_fhv.select(F.col("pickup_location_id").alias("PULocationID"))
    
    df_fhv = (df_fhv
        .withColumn("tip_amount", F.lit(0.0).cast(DoubleType()))
        .withColumn("passenger_count", F.lit(1.0).cast(DoubleType()))
        .withColumn("total_amount", F.lit(0.0).cast(DoubleType()))
        .withColumn("tipo_servicio", F.lit("fhv"))
    )
    return df_fhv

# ==============================================================================
# PASO 5: UNIFICAR DATOS DE TAXIS Y FHV
# ==============================================================================

def unificar_datos(df_taxi, df_fhv):
    """
    Unifica los datasets de taxis y FHV en un único DataFrame.
    """
    print("\n" + "=" * 60)
    print("PASO 5: Unificando datos de taxis y FHV...")
    print("=" * 60)
    
    columnas_comunes = ["PULocationID", "tip_amount", "passenger_count", "total_amount", "tipo_servicio"]
    
    df_taxi_sel = df_taxi.select(columnas_comunes)
    df_fhv_sel = df_fhv.select(columnas_comunes)
    
    df_unificado = df_taxi_sel.union(df_fhv_sel)
    
    df_unificado = df_unificado.filter(
        (F.col("PULocationID").isNotNull()) & 
        (F.col("PULocationID") > 0) &
        (F.col("PULocationID") <= 265)
    )
    
    count_total = df_unificado.count()
    count_taxi = df_unificado.filter(F.col("tipo_servicio") == "taxi").count()
    count_fhv = df_unificado.filter(F.col("tipo_servicio") == "fhv").count()
    
    print(f" Total registros unificados: {count_total:,}")
    print(f"  - Taxis: {count_taxi:,}")
    print(f"  - FHV: {count_fhv:,}")
    
    return df_unificado


# ==============================================================================
# PASO 6: CALCULAR MÉTRICAS POR ZONA
# ==============================================================================

def calcular_metricas_por_zona(df_unificado):
    """
    Calcula métricas agregadas por zona de taxi (PULocationID):
    - Propina media (tip_amount mean)
    - Número medio de pasajeros (passenger_count mean)
    - Volumen de viajes (count)
    """
    print("\n" + "=" * 60)
    print("PASO 6: Calculando métricas por zona...")
    print("=" * 60)
    
    df_metricas = df_unificado.groupBy("PULocationID").agg(
        F.avg("tip_amount").alias("propina_media"),
        F.avg("passenger_count").alias("pasajeros_medios"),
        F.count("*").alias("volumen_viajes"),
        F.sum("tip_amount").alias("propina_total"),
        F.avg("total_amount").alias("tarifa_media"),
        F.sum(F.when(F.col("tipo_servicio") == "taxi", 1).otherwise(0)).alias("viajes_taxi"),
        F.sum(F.when(F.col("tipo_servicio") == "fhv", 1).otherwise(0)).alias("viajes_fhv")
    )
    
    df_metricas = df_metricas.fillna({
        "propina_media": 0.0,
        "pasajeros_medios": 1.0,
        "propina_total": 0.0,
        "tarifa_media": 0.0
    })
    
    print("\n Preview de métricas por zona:")
    df_metricas.orderBy(F.desc("volumen_viajes")).show(10)
    
    zonas = df_metricas.count()
    print(f" Métricas calculadas para {zonas} zonas")
    
    return df_metricas


# ==============================================================================
# PASO 7: CALCULAR ÍNDICE DE PODER ADQUISITIVO
# ==============================================================================

def calcular_poder_adquisitivo(df_metricas):
    """
    Calcula el índice de poder adquisitivo aproximado usando z-scores normalizados.
    
    Fórmula:
        poder_adquisitivo = zscore(propina_media) + zscore(pasajeros_medios) + zscore(volumen_viajes)
    """
    print("\n" + "=" * 60)
    print("PASO 7: Calculando índice de poder adquisitivo...")
    print("=" * 60)
    
    # Calcular estadísticas para z-scores
    estadisticas = df_metricas.select(
        F.mean("propina_media").alias("mean_propina"),
        F.stddev("propina_media").alias("std_propina"),
        F.mean("pasajeros_medios").alias("mean_pasajeros"),
        F.stddev("pasajeros_medios").alias("std_pasajeros"),
        F.mean("volumen_viajes").alias("mean_volumen"),
        F.stddev("volumen_viajes").alias("std_volumen")
    ).collect()[0]
    
    print(f"\n Estadísticas para normalización:")
    print(f"   Propina media:    μ={estadisticas['mean_propina']:.2f}, σ={estadisticas['std_propina']:.2f}")
    print(f"   Pasajeros medios: μ={estadisticas['mean_pasajeros']:.2f}, σ={estadisticas['std_pasajeros']:.2f}")
    print(f"   Volumen viajes:   μ={estadisticas['mean_volumen']:.0f}, σ={estadisticas['std_volumen']:.0f}")
    
    # Calcular z-scores
    df_con_zscores = df_metricas.withColumn(
        "zscore_propina",
        (F.col("propina_media") - estadisticas["mean_propina"]) / estadisticas["std_propina"]
    ).withColumn(
        "zscore_pasajeros",
        (F.col("pasajeros_medios") - estadisticas["mean_pasajeros"]) / estadisticas["std_pasajeros"]
    ).withColumn(
        "zscore_volumen",
        (F.col("volumen_viajes") - estadisticas["mean_volumen"]) / estadisticas["std_volumen"]
    )
    
    df_con_zscores = df_con_zscores.fillna({
        "zscore_propina": 0.0,
        "zscore_pasajeros": 0.0,
        "zscore_volumen": 0.0
    })
    
    # Calcular índice de poder adquisitivo
    df_poder = df_con_zscores.withColumn(
        "poder_adquisitivo",
        F.col("zscore_propina") + F.col("zscore_pasajeros") + F.col("zscore_volumen")
    )
    
    # Normalizar a escala 0-100
    min_max = df_poder.select(
        F.min("poder_adquisitivo").alias("min_pa"),
        F.max("poder_adquisitivo").alias("max_pa")
    ).collect()[0]
    
    df_poder = df_poder.withColumn(
        "poder_adquisitivo_normalizado",
        ((F.col("poder_adquisitivo") - min_max["min_pa"]) / 
         (min_max["max_pa"] - min_max["min_pa"])) * 100
    ).fillna({"poder_adquisitivo_normalizado": 50.0})
    
    print(f"\n Rango de poder adquisitivo: [{min_max['min_pa']:.2f}, {min_max['max_pa']:.2f}]")
    print(" Índice de poder adquisitivo calculado y normalizado (0-100)")
    
    return df_poder

# ==============================================================================
# PASO 7b: CLUSTERING DE ZONAS CON SPARK MLLIB (KMEANS)
# ==============================================================================

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

def clustering_zonas_spark(df_metricas, n_clusters=5):
    """
    Agrupa las zonas en clusters usando KMeans de Spark MLlib.
    
    El clustering se basa en:
    - Propina media
    - Pasajeros medios
    - Volumen de viajes
    
    Args:
        df_metricas: DataFrame con métricas por zona
        n_clusters: Número de clusters (default: 5)
        
    Returns:
        DataFrame con columna 'cluster' añadida
    """
    print("\n" + "=" * 60)
    print("PASO 7b: Clustering de zonas con KMeans (Spark MLlib)...")
    print("=" * 60)
    
    # Columnas de features para clustering
    feature_cols = ["propina_media", "pasajeros_medios", "volumen_viajes"]
    
    # Rellenar nulos antes de vectorizar
    df_clean = df_metricas.fillna({
        "propina_media": 0.0,
        "pasajeros_medios": 1.0,
        "volumen_viajes": 0
    })
    
    # PASO 1: Crear vector de features
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw"
    )
    df_vectorized = assembler.transform(df_clean)
    
    # PASO 2: Escalar features (importante para KMeans)
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    scaler_model = scaler.fit(df_vectorized)
    df_scaled = scaler_model.transform(df_vectorized)
    
    # PASO 3: Entrenar modelo KMeans
    print(f"\n🔄 Entrenando KMeans con k={n_clusters} clusters...")
    
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster",
        k=n_clusters,
        seed=42,
        maxIter=100
    )
    
    modelo_kmeans = kmeans.fit(df_scaled)
    
    # PASO 4: Predecir clusters
    df_clustered = modelo_kmeans.transform(df_scaled)
    
    # PASO 5: Evaluar clustering (Silhouette Score)
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette"
    )
    silhouette = evaluator.evaluate(df_clustered)
    
    print(f"✓ Modelo KMeans entrenado")
    print(f"✓ Silhouette Score: {silhouette:.4f}")
    print(f"✓ Centros de clusters:")
    
    # Mostrar centros de clusters
    centers = modelo_kmeans.clusterCenters()
    for i, center in enumerate(centers):
        print(f"   Cluster {i}: propina={center[0]:.2f}, pasajeros={center[1]:.2f}, volumen={center[2]:.2f}")
    
    # Contar zonas por cluster
    print(f"\n📊 Distribución de zonas por cluster:")
    df_clustered.groupBy("cluster").count().orderBy("cluster").show()
    
    # Seleccionar columnas relevantes (sin vectores)
    columnas_resultado = [col for col in df_metricas.columns] + ["cluster"]
    df_resultado = df_clustered.select(
        *[c for c in columnas_resultado if c in df_clustered.columns]
    )
    
    return df_resultado, modelo_kmeans


# ==============================================================================
# PASO 7b: CLUSTERING DE ZONAS CON KMEANS
# ==============================================================================

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

def clustering_zonas_spark(df_metricas, n_clusters=5):
    """
    Agrupa las zonas en clusters usando KMeans de Spark MLlib.
    
    El clustering se basa en:
    - Propina media
    - Pasajeros medios
    - Volumen de viajes
    
    Args:
        df_metricas: DataFrame con métricas por zona
        n_clusters: Número de clusters (default: 5)
        
    Returns:
        DataFrame con columna 'cluster' añadida
    """
    print("\n" + "=" * 60)
    print("PASO 7b: Clustering de zonas con KMeans (Spark MLlib)...")
    print("=" * 60)
    
    # Columnas de features para clustering
    feature_cols = ["propina_media", "pasajeros_medios", "volumen_viajes"]
    
    # Rellenar nulos antes de vectorizar
    df_clean = df_metricas.fillna({
        "propina_media": 0.0,
        "pasajeros_medios": 1.0,
        "volumen_viajes": 0
    })
    
    # PASO 1: Crear vector de features
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw"
    )
    df_vectorized = assembler.transform(df_clean)
    
    # PASO 2: Escalar features (importante para KMeans)
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    scaler_model = scaler.fit(df_vectorized)
    df_scaled = scaler_model.transform(df_vectorized)
    
    # PASO 3: Entrenar modelo KMeans
    print(f"\n🔄 Entrenando KMeans con k={n_clusters} clusters...")
    
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster",
        k=n_clusters,
        seed=42,
        maxIter=100
    )
    
    modelo_kmeans = kmeans.fit(df_scaled)
    
    # PASO 4: Predecir clusters
    df_clustered = modelo_kmeans.transform(df_scaled)
    
    # PASO 5: Evaluar clustering (Silhouette Score)
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette"
    )
    silhouette = evaluator.evaluate(df_clustered)
    
    print(f"✓ Modelo KMeans entrenado")
    print(f"✓ Silhouette Score: {silhouette:.4f}")
    print(f"✓ Centros de clusters:")
    
    # Mostrar centros de clusters
    centers = modelo_kmeans.clusterCenters()
    for i, center in enumerate(centers):
        print(f"   Cluster {i}: propina={center[0]:.2f}, pasajeros={center[1]:.2f}, volumen={center[2]:.2f}")
    
    # Contar zonas por cluster
    print(f"\n📊 Distribución de zonas por cluster:")
    df_clustered.groupBy("cluster").count().orderBy("cluster").show()
    
    # Seleccionar columnas relevantes (sin vectores)
    columnas_resultado = [col for col in df_metricas.columns] + ["cluster"]
    df_resultado = df_clustered.select(
        *[c for c in columnas_resultado if c in df_clustered.columns]
    )
    
    return df_resultado, modelo_kmeans


def encontrar_k_optimo(df_metricas, k_range=range(2, 10)):
    """
    Encuentra el número óptimo de clusters usando el método del codo y Silhouette.
    
    Args:
        df_metricas: DataFrame con métricas
        k_range: Rango de valores de k a probar
        
    Returns:
        DataFrame con métricas para cada k
    """
    print("\n" + "=" * 60)
    print("Buscando número óptimo de clusters (Elbow Method)...")
    print("=" * 60)
    
    feature_cols = ["propina_media", "pasajeros_medios", "volumen_viajes"]
    
    df_clean = df_metricas.fillna({
        "propina_media": 0.0,
        "pasajeros_medios": 1.0,
        "volumen_viajes": 0
    })
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    df_vectorized = assembler.transform(df_clean)
    
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    df_scaled = scaler.fit(df_vectorized).transform(df_vectorized)
    
    resultados = []
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster")
    
    for k in k_range:
        kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=42)
        modelo = kmeans.fit(df_scaled)
        
        # Calcular WSSSE (Within Set Sum of Squared Errors)
        wssse = modelo.summary.trainingCost
        
        # Calcular Silhouette
        predictions = modelo.transform(df_scaled)
        silhouette = evaluator.evaluate(predictions)
        
        resultados.append({"k": k, "wssse": wssse, "silhouette": silhouette})
        print(f"  k={k}: WSSSE={wssse:.2f}, Silhouette={silhouette:.4f}")
    
    # Mejor k por Silhouette
    mejor = max(resultados, key=lambda x: x["silhouette"])
    print(f"\n✓ K óptimo sugerido: {mejor['k']} (Silhouette={mejor['silhouette']:.4f})")
    
    return resultados, mejor["k"]

# ==============================================================================
# PASO 8: DESCARGAR A PANDAS
# ==============================================================================

def convertir_a_pandas(df_spark) -> pd.DataFrame:
    """
    Convierte el DataFrame de Spark a pandas para visualización.
    """
    print("\n" + "=" * 60)
    print("PASO 8: Descargando resultado a pandas...")
    print("=" * 60)
    
    df_pandas = df_spark.toPandas()
    df_pandas = df_pandas.rename(columns={"PULocationID": "LocationID"})
    
    print(f"✓ DataFrame convertido a pandas: {len(df_pandas)} filas, {len(df_pandas.columns)} columnas")
    print(f"\n📋 Columnas disponibles: {list(df_pandas.columns)}")
    
    return df_pandas


# ==============================================================================
# PASO 9: CARGAR SHAPEFILE DE ZONAS DE TAXI
# ==============================================================================

def descargar_shapefile_zonas() -> gpd.GeoDataFrame:
    """
    Descarga y carga el shapefile oficial de zonas de taxi de NYC.
    """
    print("\n" + "=" * 60)
    print("PASO 9: Cargando shapefile de zonas de taxi NYC...")
    print("=" * 60)
    
    shapefile_dir = "taxi_zones"
    shapefile_path = os.path.join(shapefile_dir, "taxi_zones.shp")
    
    if not os.path.exists(shapefile_path):
        print(f"📥 Descargando shapefile desde TLC NYC...")
        
        try:
            response = requests.get(TAXI_ZONES_URL, timeout=30)
            response.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                z.extractall(shapefile_dir)
            
            print(f"✓ Shapefile descargado y extraído en '{shapefile_dir}/'")
            
        except Exception as e:
            print(f"⚠ Error descargando shapefile: {e}")
            alt_url = "https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=Shapefile"
            try:
                response = requests.get(alt_url, timeout=30)
                response.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    z.extractall(shapefile_dir)
                print(f"✓ Shapefile descargado desde fuente alternativa")
            except:
                raise Exception("No se pudo descargar el shapefile de zonas de taxi")
    else:
        print(f"✓ Shapefile encontrado en caché: '{shapefile_path}'")
    
    gdf = gpd.read_file(shapefile_path)
    
    if "OBJECTID" in gdf.columns and "LocationID" not in gdf.columns:
        gdf = gdf.rename(columns={"OBJECTID": "LocationID"})
    
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    
    print(f"✓ Shapefile cargado: {len(gdf)} zonas")
    print(f"✓ CRS: {gdf.crs}")
    print(f"✓ Columnas: {list(gdf.columns)}")
    
    print(f"\n📍 Preview de zonas:")
    print(gdf[["LocationID", "zone", "borough"]].head(10).to_string())
    
    return gdf


# ==============================================================================
# PASO 10: JOIN ENTRE SHAPEFILE Y DATOS
# ==============================================================================

def hacer_join_espacial(gdf_zonas: gpd.GeoDataFrame, df_metricas: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Realiza el join entre el shapefile de zonas y las métricas calculadas.
    """
    print("\n" + "=" * 60)
    print("PASO 10: Realizando join espacial...")
    print("=" * 60)
    
    gdf_zonas["LocationID"] = gdf_zonas["LocationID"].astype(int)
    df_metricas["LocationID"] = df_metricas["LocationID"].astype(int)
    
    gdf_merged = gdf_zonas.merge(df_metricas, on="LocationID", how="left")
    
    columnas_metricas = [
        "propina_media", "pasajeros_medios", "volumen_viajes",
        "poder_adquisitivo", "poder_adquisitivo_normalizado"
    ]
    
    for col in columnas_metricas:
        if col in gdf_merged.columns:
            gdf_merged[col] = gdf_merged[col].fillna(0)
    
    zonas_con_datos = gdf_merged[gdf_merged["volumen_viajes"] > 0].shape[0]
    zonas_sin_datos = gdf_merged[gdf_merged["volumen_viajes"] == 0].shape[0]
    
    print(f"✓ Join completado:")
    print(f"  - Zonas con datos: {zonas_con_datos}")
    print(f"  - Zonas sin datos: {zonas_sin_datos}")
    print(f"  - Total zonas: {len(gdf_merged)}")
    
    return gdf_merged


# ==============================================================================
# PASO 11: CREAR MAPA COROPLÉTICO CON FOLIUM
# ==============================================================================

def crear_mapa_coropletico(gdf: gpd.GeoDataFrame) -> folium.Map:
    """
    Crea un mapa coroplético interactivo de NYC con poder adquisitivo por zona.
    """
    print("\n" + "=" * 60)
    print("PASO 11: Creando mapa coroplético...")
    print("=" * 60)
    
    nyc_center = [40.7128, -74.0060]
    
    mapa = folium.Map(
        location=nyc_center,
        zoom_start=10,
        tiles="cartodbpositron",
        control_scale=True
    )
    
    min_val = gdf["poder_adquisitivo_normalizado"].min()
    max_val = gdf["poder_adquisitivo_normalizado"].max()
    
    colormap = cm.LinearColormap(
        colors=["#f7fbff", "#6baed6", "#2171b5", "#08306b"],
        vmin=min_val,
        vmax=max_val,
        caption="Índice de Poder Adquisitivo (0-100)"
    )
    
    def style_function(feature):
        valor = feature["properties"].get("poder_adquisitivo_normalizado", 0)
        return {
            "fillColor": colormap(valor) if valor else "#gray",
            "color": "#333333",
            "weight": 0.5,
            "fillOpacity": 0.7
        }
    
    def highlight_function(feature):
        return {
            "fillColor": "#ffff00",
            "color": "#000000",
            "weight": 2,
            "fillOpacity": 0.9
        }
    
    tooltip = folium.GeoJsonTooltip(
        fields=[
            "zone", 
            "borough", 
            "propina_media", 
            "volumen_viajes",
            "poder_adquisitivo_normalizado"
        ],
        aliases=[
            "Zona:", 
            "Borough:", 
            "Propina Media ($):", 
            "Num Viajes:",
            "Poder Adquisitivo:"
        ],
        localize=True,
        sticky=True,
        style="""
            background-color: white;
            border: 2px solid #333;
            border-radius: 5px;
            box-shadow: 3px 3px 3px rgba(0,0,0,0.3);
            font-size: 12px;
            padding: 10px;
        """
    )
    
    geojson_layer = folium.GeoJson(
        gdf,
        name="Poder Adquisitivo por Zona",
        style_function=style_function,
        highlight_function=highlight_function,
        tooltip=tooltip
    )
    
    geojson_layer.add_to(mapa)
    colormap.add_to(mapa)
    folium.LayerControl().add_to(mapa)
    
    titulo_html = """
    <div style="position: fixed; 
                top: 10px; 
                left: 50px; 
                z-index: 9999; 
                background-color: white;
                padding: 15px;
                border: 2px solid #333;
                border-radius: 10px;
                box-shadow: 3px 3px 5px rgba(0,0,0,0.3);
                font-family: Arial, sans-serif;">
        <h3 style="margin: 0; color: #333;">
            Mapa de Poder Adquisitivo - NYC Taxi Zones
        </h3>
        <p style="margin: 5px 0 0 0; color: #666; font-size: 12px;">
            Basado en propinas, pasajeros y volumen de viajes (2023)
        </p>
    </div>
    """
    mapa.get_root().html.add_child(folium.Element(titulo_html))
    
    minimap = plugins.MiniMap(toggle_display=True)
    mapa.add_child(minimap)
    
    plugins.Fullscreen(
        position="topleft",
        title="Pantalla completa",
        title_cancel="Salir de pantalla completa",
        force_separate_button=True
    ).add_to(mapa)
    
    print("✓ Mapa coroplético creado")
    
    return mapa


# ==============================================================================
# PASO 12: AGREGAR CAPAS ADICIONALES (BONUS)
# ==============================================================================

def agregar_capas_adicionales(mapa: folium.Map, gdf: gpd.GeoDataFrame) -> folium.Map:
    """
    Agrega capas adicionales opcionales al mapa.
    """
    print("\n" + "=" * 60)
    print("PASO 12 (BONUS): Agregando capas adicionales...")
    print("=" * 60)
    
    aeropuertos = folium.FeatureGroup(name="Aeropuertos")
    
    aeropuertos_data = [
        {"nombre": "JFK International", "lat": 40.6413, "lon": -73.7781, "zona": 132},
        {"nombre": "LaGuardia", "lat": 40.7769, "lon": -73.8740, "zona": 138},
        {"nombre": "Newark (EWR)", "lat": 40.6895, "lon": -74.1745, "zona": 1},
    ]
    
    for aeropuerto in aeropuertos_data:
        folium.Marker(
            location=[aeropuerto["lat"], aeropuerto["lon"]],
            popup=f"<b>{aeropuerto['nombre']}</b><br>Zone ID: {aeropuerto['zona']}",
            icon=folium.Icon(color="red", icon="plane", prefix="fa"),
            tooltip=aeropuerto["nombre"]
        ).add_to(aeropuertos)
    
    aeropuertos.add_to(mapa)
    
    top_zonas = folium.FeatureGroup(name="Top 10 Poder Adquisitivo")
    
    top_10 = gdf.nlargest(10, "poder_adquisitivo_normalizado")
    
    for _, row in top_10.iterrows():
        centroid = row.geometry.centroid
        folium.CircleMarker(
            location=[centroid.y, centroid.x],
            radius=10,
            popup=f"<b>{row.get('zone', 'N/A')}</b><br>"
                  f"Poder Adquisitivo: {row['poder_adquisitivo_normalizado']:.1f}",
            color="gold",
            fill=True,
            fillColor="gold",
            fillOpacity=0.8,
            tooltip=f"Top: {row.get('zone', 'Top Zone')}"
        ).add_to(top_zonas)
    
    top_zonas.add_to(mapa)
    folium.LayerControl(collapsed=False).add_to(mapa)
    
    print("✓ Capas adicionales añadidas")
    
    return mapa


# ==============================================================================
# PASO 13: GUARDAR MAPA HTML
# ==============================================================================

def guardar_mapa(mapa: folium.Map, output_path: str):
    """
    Guarda el mapa como archivo HTML.
    """
    print("\n" + "=" * 60)
    print("PASO 13: Guardando mapa...")
    print("=" * 60)
    
    mapa.save(output_path)
    
    file_size = os.path.getsize(output_path) / (1024 * 1024)
    
    print(f"✓ Mapa guardado: {output_path}")
    print(f"✓ Tamaño: {file_size:.2f} MB")
    print(f"\nAbre el archivo en un navegador para visualizar el mapa interactivo")


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Función principal que orquesta todo el pipeline de análisis.
    """
    print("\n" + "=" * 80)
    print("  ANÁLISIS DE PODER ADQUISITIVO POR ZONA - NYC TAXI DATA 2023")
    print("=" * 80)
    
    try:
        spark = crear_spark_session()
        df_taxi = cargar_datos_taxi(spark)
        df_fhv = cargar_datos_fhv(spark)
        df_unificado = unificar_datos(df_taxi, df_fhv)
        df_metricas = calcular_metricas_por_zona(df_unificado)
        df_poder = calcular_poder_adquisitivo(df_metricas)
        df_pandas = convertir_a_pandas(df_poder)
        gdf_zonas = descargar_shapefile_zonas()
        gdf_merged = hacer_join_espacial(gdf_zonas, df_pandas)
        mapa = crear_mapa_coropletico(gdf_merged)
        mapa = agregar_capas_adicionales(mapa, gdf_merged)
        guardar_mapa(mapa, OUTPUT_FILE)
        
        spark.stop()
        print("\n✅ Sesión de Spark cerrada")
        
        print("\n" + "=" * 80)
        print("  ✅ ANÁLISIS COMPLETADO EXITOSAMENTE")
        print("=" * 80)
        
        print(f"""
RESUMEN DEL ANÁLISIS:
────────────────────────────────────────────────────────────
  - Zonas analizadas: {len(gdf_merged)}
  - Total de viajes procesados: {df_pandas['volumen_viajes'].sum():,.0f}
  - Propina media global: ${df_pandas['propina_media'].mean():.2f}

TOP 5 ZONAS POR PODER ADQUISITIVO:
""")
        
        top5 = gdf_merged.nlargest(5, "poder_adquisitivo_normalizado")[
            ["zone", "borough", "poder_adquisitivo_normalizado", "volumen_viajes"]
        ]
        print(top5.to_string(index=False))
        
        print(f"""
────────────────────────────────────────────────────────────
Archivo generado: {OUTPUT_FILE}
────────────────────────────────────────────────────────────
""")
        
        return gdf_merged
        
    except Exception as e:
        print(f"\n❌ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    resultado = main()