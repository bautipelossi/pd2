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
    
    columnas_taxi = ["PULocationID", #numérica a la vista pero categórica en ralidad: NO tratar como numérica
                    "tpep_pickup_datetime", 
                    "tip_amount",
                    "passenger_count",
                    "fare_amount",
                    "total_amount",
                    "trip_distance"
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
    print("=" * 60 )

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

def crear_features_avanzadas(df, df_restaurantes=None):
    """
    Crea features avanzadas A PRIORI basadas en históricos
    """

    print("\n🚀 Creando features avanzadas...")

    # ------------------------------------------------------------------
    # 1. PROPINA MEDIA POR ZONA
    # ------------------------------------------------------------------
    avg_tip_zone = df.groupBy("PULocationID").agg(
        F.mean("tip_amount").alias("avg_tip_zone")
    )

    df = df.join(avg_tip_zone, on="PULocationID", how="left")

    # ------------------------------------------------------------------
    # 2. PROPINA MEDIA POR ZONA + HORA
    # ------------------------------------------------------------------
    avg_tip_zone_hour = df.groupBy("PULocationID", "hour").agg(
        F.mean("tip_amount").alias("avg_tip_zone_hour")
    )

    df = df.join(avg_tip_zone_hour, on=["PULocationID", "hour"], how="left")

    # ------------------------------------------------------------------
    # 3. ESTIMACIÓN DE DURACIÓN (SI EXISTE)
    # ------------------------------------------------------------------
    if "trip_duration" in df.columns:
        avg_duration = df.groupBy("PULocationID").agg(
            F.mean("trip_duration").alias("avg_duration_zone")
        )
        df = df.join(avg_duration, on="PULocationID", how="left")

    # ------------------------------------------------------------------
    # 4. DENSIDAD DE RESTAURANTES (SI SE PROPORCIONA DATASET)
    # ------------------------------------------------------------------
    """if df_restaurantes is not None:

        # Se asume que tienes columna "PULocationID" o equivalente
        restaurants_by_zone = df_restaurantes.groupBy("PULocationID").agg(
            F.count("*").alias("num_restaurants")
        )

        df = df.join(restaurants_by_zone, on="PULocationID", how="left")

        # Rellenar nulos (zonas sin restaurantes)
        df = df.fillna({"num_restaurants": 0})"""

    # ------------------------------------------------------------------
    # 5. TIPO DE ZONA (HEURÍSTICA DE NEGOCIO)
    # ------------------------------------------------------------------

    # ⚠️ Ejemplo simplificado → puedes refinarlo luego
    # IDs reales puedes afinarlos con el shapefile si quieres

    df = df.withColumn(
        "zone_type",
        F.when(F.col("PULocationID").isin([132, 138]), "airport")  # JFK, LaGuardia aprox
        .when(F.col("PULocationID").isin([161, 162, 163, 164]), "manhattan_core")
        .when(F.col("PULocationID") < 100, "residential")
        .otherwise("other")
    )

    # Codificación simple (numérica)
    df = df.withColumn(
        "zone_type_index",
        F.when(F.col("zone_type") == "airport", 3)
        .when(F.col("zone_type") == "manhattan_core", 2)
        .when(F.col("zone_type") == "residential", 1)
        .otherwise(0)
    )

    # ------------------------------------------------------------------
    # 🔹 6. RELLENO DE NULOS IMPORTANTES
    # ------------------------------------------------------------------
    df = df.fillna({
        "avg_tip_zone": 0,
        "avg_tip_zone_hour": 0
    })

    print("✅ Features avanzadas creadas")

    return df

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
        F.count("*").alias("num_trips"),
        F.mean("fare_amount").alias("avg_fare"),
        F.mean("tip_pct").alias("avg_tip_pct")
    ).orderBy("passenger_count").show()

# Estudio y análisis de outliers
import matplotlib.pyplot as plt
import seaborn as sns
def generar_boxplots(df_spark, columnas, output_dir):
    """
    Genera boxplots para cada variable numérica y los guarda en disco
    """
    print("\n📊 Generando boxplots...")

    # Convertimos a pandas (solo columnas necesarias)
    df_pd = df_spark.select(columnas).toPandas()

    os.makedirs(output_dir, exist_ok=True)

    for col in columnas:
        plt.figure(figsize=(6, 4))
        sns.boxplot(x=df_pd[col])
        plt.title(f"Boxplot - {col}")

        filepath = os.path.join(output_dir, f"boxplot_{col}.png")
        plt.savefig(filepath)
        plt.close()

    print(f"✅ Boxplots guardados en: {output_dir}")


def tratar_outliers(df_spark):
    """
    Genera boxplots y elimina outliers usando método IQR
    """

    print("\n🚀 Tratamiento de outliers iniciado...")

    # Columnas numéricas a analizar
    columnas = [
        "tip_amount",
        "fare_amount",
        "total_amount",
        "trip_distance"
    ]

    # Ruta de salida (ajustada a tu estructura)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    output_dir = os.path.join(project_root, "src", "Visualizacion", "Boxplots_Propinas")

    # 1) Generar boxplots ANTES de limpiar
    #generar_boxplots(df_spark, columnas, output_dir)

    # 2) Cálculo de límites IQR
    bounds = {}

    for col in columnas:
        q1, q3 = df_spark.approxQuantile(col, [0.25, 0.75], 0.01)
        iqr = q3 - q1

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        bounds[col] = (lower, upper)


    # 3) Filtrado de outliers
    df_clean = df_spark

    for col in columnas:
        lower, upper = bounds[col]
        df_clean = df_clean.filter(
            (F.col(col) >= lower) & (F.col(col) <= upper)
        )

    print("✅ Outliers eliminados correctamente")

    return df_clean


# ==============================================================================
# PASO 6: BASELINE MODEL
# ==============================================================================

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

def base_pipeline():

    # Variables -a priori-
    categorical_cols = [
        "PULocationID", 
        "day_of_week", 
        "zone_type"
    ]

    analytical_cols = [
        "passenger_count", 
        "fare_amount", 
        "hour", 
        "month",
        "is_weekend",
        "avg_tip_zone",
        "avg_tip_zone_hour",
        "avg_duration_zone"
    ]

    indexers = [
        StringIndexer(inputCol = col, outputCol = f"{col}_idx", handleInvalid="keep")
        for col in categorical_cols
    ]

    encoders = [
        OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_ohe")
        for col in categorical_cols
    ]
    
    assembler = VectorAssembler(
        inputCols = [f"{col}_ohe" for col in categorical_cols] + analytical_cols,
        outputCol = "features"
    )

    # Modelo Base
    lr = LinearRegression(
        featuresCol = "features",
        labelCol = "tip_pct",
        predictionCol = "prediction",
        maxIter = 50,
        regParam = 0.1,
        elasticNetParam = 0.0
    )

    pipeline = Pipeline(
        stages = indexers + encoders + [assembler, lr]
    )

    return pipeline

# ==============================================================================
# PASO 7: PRODUCTION MODEL
# ==============================================================================
from pyspark.ml.regression import GBTRegressor

def production_pipeline():

    # Variables -a priori-
    categorical_cols = [
        "PULocationID", 
        "day_of_week", 
        "zone_type"
    ]

    numeric_cols = [
        "passenger_count", 
        "fare_amount", 
        "hour", 
        "month",
        "is_weekend",
        "avg_tip_zone",
        "avg_tip_zone_hour",
        "avg_duration_zone"
    ]

    # Indexación
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        for col in categorical_cols
    ]

    # One Hot Encoding
    encoders = [
        OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_ohe")
        for col in categorical_cols
    ]

    # Ensamblado
    assembler = VectorAssembler(
        inputCols=[f"{col}_ohe" for col in categorical_cols] + numeric_cols,
        outputCol="features"
    )

    # MODELO FINAL
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="tip_pct",
        predictionCol="prediction",
        maxIter=30,
        maxDepth=3,
        stepSize=0.1,
        subsamplingRate=0.8,
        seed=42
    )

    pipeline = Pipeline(
        stages=indexers + encoders + [assembler, gbt]
    )

    return pipeline

# ============================================
# PASO 8: FUNCIÓN PIPELINE COMPLETO
# ============================================
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

    print("Evaluación del modelo:")
    print(f"RMSE: {rmse:.4f}")
    print(f"MAE: {mae:.4f}")
    print(f"R2: {r2:.4f}")

    return predictions

def run_model(df, modelo):
    """
    Ejecuta todo el flujo:
    preparación, split, entrenamiento y evaluación
    """
    
    if modelo == "baseline":
        print("🚀 Starting Linear Regression Baseline...")
    else:
        print("🚀 Starting Gradient Boost Tree...")
    
    # 1. Preparar datos
    df = df.drop("tip_amount", "total_amount")
    
    # 2. Split
    train_df, test_df = data_split(df)
    
    # 3. Pipeline
    if modelo == "baseline":
        pipeline = base_pipeline()
    else:
        pipeline = production_pipeline()
    
    # 4. Entrenar
    model = train_model(pipeline, train_df)
    
    # 5. Evaluar
    predictions = evaluate_model(model, test_df)
    
    if modelo == "baseline":
        print("✅ Baseline model completed")
    else:
        print("✅ Production model completed")
    
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
        df_final = crear_features_avanzadas(df_final)
        df_final = tratar_outliers(df_final)

        estudio_analítico(df_final)

        base_model, base_predicts = run_model(df_final, "baseline")
        prod_model, prod_predicts = run_model(df_final, "gbt")
        
        spark.stop()
        
    except Exception as e:
        print(f"\n❌ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    resultado = main()