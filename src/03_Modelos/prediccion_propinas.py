'''
    ============================================================================
    MODELO PREDICTOR DE PROPINA A PRIORI
    ============================================================================

    Este script carga los datos y entrena un modelo de Aprendizaje Automático
    para predecir la propina que se dará al conductor en un determinado viaje,
    antes de que se realice dicho viaje (funcionará tanto para predecir propinas
    para LTC y FHV). 

'''


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
    "taxi": r"C:\Users\rodri\pd2\Entrega1_Pd2\datos\limpios\nyc_taxi_clean.parquet",
    "fhv": r"C:\Users\rodri\pd2\Entrega1_Pd2\datos\limpios\fhv_2023_clean.parquet"
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
    
    print(f"✓ Spark Session creada: {spark.version}")
    print(f"✓ Endpoint MinIO: {MINIO_CONFIG['endpoint']}")
    
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
        print(f"✓ {nombre_clave.capitalize()} cargado exitosamente desde MinIO.")
        return df
    except Exception as e:
        print(f"⚠ Fallo en MinIO: {str(e).splitlines()[0]}") # Solo mostramos la primera línea del error
        
    # 2. Intento con Local (Backup)
    try:
        print(f"Recurriendo al backup local: {LOCAL_PATHS[nombre_clave]}...")
        df = spark.read.parquet(LOCAL_PATHS[nombre_clave])
        print(f"✓ {nombre_clave.capitalize()} cargado desde disco local.")
        return df
    except Exception as e:
        print(f"❌ Error crítico: No se encontró el dataset en MinIO ni en Local.")
        raise e

def cargar_datos_taxi(spark: SparkSession):
    df_taxi = cargar_dataset(spark, "taxi")
    
    # --- NORMALIZACIÓN DE COLUMNAS ---
    # Si viene en minúsculas (pulocationid), la renombramos a CamelCase (PULocationID)
    if "pulocationid" in df_taxi.columns:
        df_taxi = df_taxi.withColumnRenamed("pulocationid", "PULocationID")
    
    # Seleccionamos las columnas que nos servirán para el estudio A PRIORI
    columnas_taxi = ["PULocationID", 
                    "tpep_pickup_datetime", 
                    "tip_amount",
                    "passenger_count",
                    "fare_amount",
                    "total_amount"]

    # Verificamos qué columnas existen realmente (en el log dice que tienes tip_amount, total_amount, etc.)
    columnas_existentes = [col for col in columnas_taxi if col in df_taxi.columns]
    
    # Eliminamos posibles valores nulos o inválidos
    df = df.filter(F.col("tip_amount") >= 0)
    df = df.filter(F.col("fare_amount") > 0)
    df = df.filter(F.col("total_amount") > 0)

    df_taxi = df_taxi.select(columnas_existentes)
    df_taxi = df_taxi.withColumn("tipo_servicio", F.lit("taxi"))
    
    print(f"✓ Datos de taxi normalizados y cargados.")
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
# PASO 5: FEATURE ENGINEERING
# ==============================================================================

def creacion_variables(df_taxi):
    print("\n" + "=" * 60 )
    print("PASO 5: Creando variables auxiliares para el modelo ...")
    print("\n" + "=" * 60 )

    # Creación de variables auxiliares temporales
    df_auxt = df_taxi.withColumn(
        "hour", F.hour("PULocationID")
    ).withColumn(
        "day_of_week", F.dayofweek("PULocationID")
    ).withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), True).
        otherwise(False)
    )

    # Creación de variables auxiliares cuantitativas
    df_final = df_auxt.withColumn(
        "tip_pct", (col("tip_amount") / col("fare_amount")) * 100
    )

    return df_final

def estudio_analítico(df_final):
    print("\n" + "=" * 60 )
    print(" Estudio analítico del df final ...")
    print("\n" + "=" * 60 )

    df

