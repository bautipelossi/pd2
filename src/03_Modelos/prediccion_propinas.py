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
# PASO 1: IMPORTAR LIBRERÍAS Y ENTORNO (COPIADO DEL SCRIPT QUE FUNCIONA)
# ==============================================================================

import os
import sys
import warnings
from typing import Tuple
from pathlib import Path

import pandas as pd
import geopandas as gpd
import folium
from folium import plugins
import branca.colormap as cm
from scipy import stats
from dotenv import load_dotenv, find_dotenv # Añadido de tu script

# --- CONFIGURACIÓN DE ENTORNO IDÉNTICA A LA TUYA ---
load_dotenv(find_dotenv())
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#os.environ['HADOOP_HOME'] = "C:/hadoop"
#os.environ['HADOOP_TMP_DIR'] = "C:/tmp/hadoop"
# ¡HEMOS ELIMINADO LA DECLARACIÓN MANUAL DE SPARK_HOME!
# ---------------------------------------------------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

warnings.filterwarnings('ignore')

# ==============================================================================
# PASO 2: CONFIGURACIÓN DE CREDENCIALES MINIO / S3
# ==============================================================================

# Credenciales MinIO (evitar hardcodear secretos)
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "https://minio.fdi.ucm.es"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", ""),
    "secret_key": os.getenv("MINIO_SECRET_KEY", ""),
    "path_style": os.getenv("MINIO_PATH_STYLE", "true")
}

# Rutas de datos en el bucket
S3_PATHS = {
    "taxi": os.getenv("MINIO_TAXI_PATH", "s3a://pd2/taxomanos/limpios/nyc_taxi_clean.parquet"),
    "fhv": os.getenv("MINIO_FHV_PATH", "s3a://pd2/taxomanos/limpios/fhv_2023_clean.parquet")
}

# Rutas Locales (Backup)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LOCAL_PATHS = {
    "taxi": os.getenv("LOCAL_TAXI_PATH", os.path.join(PROJECT_ROOT, "datos", "limpios", "nyc_taxi_clean.parquet")),
    "fhv": os.getenv("LOCAL_FHV_PATH", os.path.join(PROJECT_ROOT, "datos", "limpios", "fhv_2023_clean.parquet"))
}

RESTAURANTS_PATH = os.getenv("RESTAURANTS_CSV_PATH", os.path.join(PROJECT_ROOT, "datos", "crudos", "restaurantes_nyc_clean.csv"))
K_DIAGNOSTIC_PNG = os.path.join(PROJECT_ROOT, "outputs", "kmeans_elbow_silhouette.png")
# URL del shapefile oficial de zonas de taxi de NYC
TAXI_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"


# ==============================================================================
# PASO 3: CREAR Y CONFIGURAR SESIÓN SPARK PARA MINIO (VERSIÓN CORREGIDA)
# ==============================================================================

def crear_spark_session() -> SparkSession:
    print("=" * 60)
    print("PASO 3: Configurando sesión de Spark para MinIO...")
    print("=" * 60)
    
    packages = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Prediccion_Propinas") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars.packages", packages) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", MINIO_CONFIG["path_style"]) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .config("spark.local.dir", os.environ.get('HADOOP_TMP_DIR', "C:/tmp/hadoop")) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"✓ Spark Session creada con éxito")
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
    #try:
    #    print(f"Intentando cargar desde MinIO: {S3_PATHS[nombre_clave]}...")
    #    df = spark.read.parquet(S3_PATHS[nombre_clave])
    #    print(f"✓ {nombre_clave.capitalize()} cargado exitosamente desde MinIO.")
    #    return df
    #except Exception as e:
    #    print(f"⚠ Fallo en MinIO: {str(e).splitlines()[0]}") # Solo mostramos la primera línea del error
        
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
    if "pulocationid" in df_taxi.columns:
        df_taxi = df_taxi.withColumnRenamed("pulocationid", "PULocationID")
    
    columnas_taxi = ["PULocationID", 
                    "tpep_pickup_datetime", 
                    "tip_amount",
                    "passenger_count",
                    "fare_amount",
                    "total_amount"
                    ]

    columnas_existentes = [col for col in columnas_taxi if col in df_taxi.columns]
    
    # --- BUG CORREGIDO (Era df_taxi, no df) ---
    df_taxi = df_taxi.filter(F.col("tip_amount") >= 0)
    df_taxi = df_taxi.filter(F.col("fare_amount") > 0)
    df_taxi = df_taxi.filter(F.col("total_amount") > 0)

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
        "hour", F.hour("tpep_pickup_datetime")
    ).withColumn(
        "day_of_week", F.dayofweek("tpep_pickup_datetime")
    ).withColumn(
        "is_weekend",
        F.when(F.col("day_of_week").isin([1, 7]), True).
        otherwise(False)
    ).withColumn(
        "month", F.month("tpep_pickup_datetime")
    )

    # Creación de variables auxiliares cuantitativas
    df_final = df_auxt.withColumn(
        "tip_pct", (F.col("tip_amount") / F.col("fare_amount")) * 100
    )

    return df_final

def estudio_analítico(df_final):
    print("\n" + "=" * 60 )
    print(" Estudio analítico del df final ...")
    print("\n" + "=" * 60 )

    # --- 1) Comprobamos las métricas de cada columna
    numeric_cols = [
                    "tip_amount", 
                    "passenger_count", 
                    "fare_amount",  
                    "total_amount", 
                    "tip_pct"
                    ]

    print("\n 1) Estadísticas descriptivas de las columnas numéricas")
    df_final.select(numeric_cols).describe().show()

    # --- 2) Análisis variable objetivo
    print("\n 2) Análisis variable objetivo ")
    
    # Quitamos los .show() del final y añadimos la F. a min y max
    df_tip_amount = df_final.select(
        F.lit("tip_amount").alias("variable"),
        F.mean("tip_amount").alias("media"),
        F.stddev("tip_amount").alias("desv_tipica"),
        F.min("tip_amount").alias("min"),
        F.max("tip_amount").alias("max")
    )

    df_tip_pct = df_final.select(
        F.lit("tip_pct").alias("variable"),
        F.mean("tip_pct").alias("media"),
        F.stddev("tip_pct").alias("desv_tipica"),
        F.min("tip_pct").alias("min"),
        F.max("tip_pct").alias("max")
    )

    # Unir verticalmente y AHORA sí mostramos el resultado
    df_tip_stats = df_tip_amount.unionByName(df_tip_pct)
    df_tip_stats.show()

    # --- 3) Estudio de correlaciones entre variables
    print("\n 3) Estudio de correlaciones entre variables numéricas y variable objetivo")
    for c in numeric_cols:
        if c != "tip_amount":
            try:
                corr_value = df_final.stats.corr("tip_amount", c)
                print(f"Correlación entre tip_amount y {c}: {corr_value:.2f} \n")
            except:
                pass
    
    # --- 4) Agregaciones temporales para ver como se distribuyen las propinas
    print("4) Agregaciones temporale para observar distribución temporal de las propinas")
    
    print("\n Por Hora")
    df_final.groupBy("hour").agg(
        F.mean("tip_amount").alias("avg_tip_amount"),
        F.count("*").alias("num_trips")
    ).orderBy("hour").show()

    print("\n Por Día de la Semana")
    df_final.groupBy("day_of_week").agg(
        F.mean("tip_amount").alias("avg_tip_amount"),
        F.count("*").alias("num_trips")
    ).orderBy("day_of_week").show()

    print("\n Por Mes")
    df_final.groupBy("month").agg(
        F.mean("tip_amount").alias("avg_tip_amount"),
        F.count("*").alias("num_trips")
    ).orderBy("month").show()

    print("\n Comparación entre semana vs fin de semana")
    # CUIDADO AQUÍ: Tu compañero agrupa por "is_weekday" pero arriba creó la columna "is_weekend"
    df_final.groupBy("is_weekend").agg(
        F.mean("tip_amount").alias("avg_tip_amount"),
        F.count("*").alias("num_trips")
    ).show()

    # --- 5) Comparación entre tarifa y comportamiento con la propina
    print("5) Comparación entre tarifa vs comportamiento con la propina")

    df_final.groupBy("passenger_count").agg(
        F.mean("fare_amount").alias("avg_fare"),
        F.mean("tip_pct").alias("avg_tip_pct")
    ).orderBy("passenger_count").show()

# ==============================================================================
# PASO 6: BASELINE MODEL
# ==============================================================================

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

def base_pipeline():

    # Variables -a priori-
    cols = [
            "PULocationID", 
            "passenger_count", 
            "fare_amount", 
            "hour",
            "day_of_week",
            "is_weekend",
            "month"
            ]
    
    assembler = VectorAssembler(
        inputCols = cols,
        outputCol = "features"
    )

    # Modelo Base
    lr = LinearRegression(
        featuresCol = "features",
        labelCol = "tip_pct",
        predictionCol = "prediction",
        maxIter = 50,
        regParam = 0.0,
        elasticNetParam = 0.0
    )

    pipeline = Pipeline(stages = [assembler, lr])

    return pipeline

def data_split(df_final, train_ratio: float = 0.8):
    """
    Divide los datos en entrenamiento y test
    """

    train_df, test_df = df_final.randomSplit([train_ratio, 1-train_ratio], seed=42)

    return train_df, test_df

def train_model(pipeline: Pipeline, train_df):
    """
    Se entrena el modelo con el conjunto de entrenamiento
    """

    model = pipeline.fit(train_df)
    return model

def evaluate_model(model, test_df):
    """
    Evalúa el modelo usando varias métricas
    """

    predictions = model.transform(test_df)

    evaluator_rmse = RegressionEvaluator(
        labelCol = "tip_pct",
        predictionCol = "prediction",
        metricName = "rmse"
    )

    evaluator_mae = RegressionEvaluator(
        labelCol = "tip_pct",
        predictionCol = "prediction",
        metricName = "mae"
    )

    evaluator_r2 = RegressionEvaluator(
        labelCol = "tip_pct",
        predictionCol = "prediction",
        metricName = "r2"
    )

    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    print("Evaluacón del modelo base:")
    print(f"RMSE: {rmse:.4f}")
    print(f"MAE: {mae:.4f}")
    print(f"R2: {r2:.4f}")

    return predictions

# ============================================
# 🔹 6. FUNCIÓN PRINCIPAL (PIPELINE COMPLETO)
# ============================================
def run_linear_regression_baseline(df):
    """
    Ejecuta todo el flujo:
    preparación, split, entrenamiento y evaluación
    """
    
    print("🚀 Starting Linear Regression Baseline...")
    
    # 1. Preparar datos
    df = df.drop("tip_amount", "total_amount")
    
    # 2. Split
    train_df, test_df = data_split(df)
    
    # 3. Pipeline
    pipeline = base_pipeline()
    
    # 4. Entrenar
    model = train_model(pipeline, train_df)
    
    # 5. Evaluar
    predictions = evaluate_model(model, test_df)
    
    print("✅ Baseline model completed")
    
    return model, predictions


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
        #df_fhv = cargar_datos_fhv(spark)
        #df_unificado = unificar_datos(df_taxi, df_fhv)
        #df_metricas = calcular_metricas_por_zona(df_unificado)
        #df_poder = calcular_poder_adquisitivo(df_metricas)
        df_final = creacion_variables(df_taxi)

        estudio_analítico(df_final)

        model, predictions = run_linear_regression_baseline(df_final)
        
        spark.stop()
        
    except Exception as e:
        print(f"\n❌ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    resultado = main()