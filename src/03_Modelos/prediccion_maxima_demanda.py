import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path  # Añadimos esta librería para poder construir la ruta local de forma dinámica
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, count, date_trunc
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row


def create_spark_session():
    load_dotenv(find_dotenv())
    
    # Configuramos las rutas directamente en el sistema antes de arrancar Spark
    os.environ['HADOOP_HOME'] = "C:/hadoop"
    # Forzamos a que use C:/tmp/hadoop para los bloques de S3
    os.environ['HADOOP_TMP_DIR'] = "C:/tmp/hadoop"

    spark = SparkSession.builder \
        .appName("Prediccion_Demanda_Taxi_Ex1a") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.buffer.dir", "C:/tmp/hadoop") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array") \
        .getOrCreate()
    
    return spark

def predict_max_demand_zone(spark, model, target_day, target_hour):
    """
    Dada una hora y un día de la semana, predice qué zona tendrá más demanda.
    target_day: int (ej. 1 para Domingo, 2 para Lunes... según codificación de Spark)
    target_hour: int (0 a 23)
    """
    print(f"\n--- Prediciendo demanda para el Día {target_day} a las {target_hour}:00 ---")

    zonas_ids = range(1, 264)
    
    # --------> AQUÍ ESTÁ EL CAMBIO (int(z) en lugar de float(z)) <--------
    data_grid = [Row(pulocationid=int(z), day_of_week=int(target_day), pickup_hour=int(target_hour)) for z in zonas_ids]
    
    df_pred_input = spark.createDataFrame(data_grid)

    predicciones = model.transform(df_pred_input)
    top_zona = predicciones.orderBy(col("prediction").desc()).first()

    if top_zona:
        print(f"LA ZONA RECOMENDADA ES: {int(top_zona['pulocationid'])}")
        print(f"Viajes esperados (predicción): {round(top_zona['prediction'], 2)}")
    else:
        print("No se pudo realizar la predicción.")


def prepare_data(spark):
    """Carga los datos agrupados dinámicamente desde MinIO o desde local como fallback"""
    load_dotenv(find_dotenv())
    minio_bucket = os.getenv("MINIO_BUCKET")
    minio_groupPath = os.getenv("MINIO_GROUP_PATH")
    
    assert minio_bucket, "Falta MINIO_BUCKET en el entorno/.env"
    assert minio_groupPath, "Falta MINIO_GROUP_PATH en el entorno/.env"
    
    # Construimos la ruta dinámicamente con las variables de entorno
    ruta_parquet = f"s3a://{minio_bucket}/{minio_groupPath}/resumen_zona_hora.parquet"
    
    # Definimos la ruta local basándonos en la estructura de nuestro repositorio
    base_dir = Path(__file__).resolve()
    project_root = base_dir.parents[2]
    ruta_local = project_root / "Entrega1_Pd2" / "datos" / "limpios" / "resumen_zona_hora.parquet"

    # Añadimos un bloque try-except para intentar cargar desde la nube y asegurar una alternativa local
    try:
        print(f"Intentando leer datos desde MinIO: {ruta_parquet}")
        df_grouped = spark.read.parquet(ruta_parquet)
        
        # Forzamos una acción (count) para que Spark evalúe la lectura y salte al except si MinIO falla
        df_grouped.count() 
    except Exception as e:
        print(f"Fallo de conexión con MinIO detectado: {str(e).splitlines()[0]}")
        print(f"Hacemos fallback y leemos nuestro archivo local desde: {ruta_local}")
        df_grouped = spark.read.parquet(str(ruta_local))

    if "day_of_week" not in df_grouped.columns and "date_only" in df_grouped.columns:
        df_grouped = df_grouped.withColumn("day_of_week", dayofweek("date_only"))

    return df_grouped.dropna()


def train_pipeline(df):
    """Crea el pipeline y entrena el modelo de regresión"""
    indexer_zone = StringIndexer(inputCol="pulocationid", outputCol="zone_idx", handleInvalid="keep")
    indexer_day = StringIndexer(inputCol="day_of_week", outputCol="day_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["zone_idx", "day_idx"], outputCols=["zone_vec", "day_vec"])
    assembler = VectorAssembler(inputCols=["pickup_hour", "zone_vec", "day_vec"], outputCol="features")
    rf = RandomForestRegressor(featuresCol="features", labelCol="demanda_viajes", numTrees=50, maxDepth=10)

    pipeline = Pipeline(stages=[indexer_zone, indexer_day, encoder, assembler, rf])
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    print("Entrenando el modelo RandomForest en el clúster...")
    model = pipeline.fit(train_data)


    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="demanda_viajes", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"Error Cuadrático Medio (RMSE): {rmse} viajes por zona/hora")

    return model



if __name__ == "__main__":
    spark = create_spark_session()


    print("Preparando datos...")
    dataset = prepare_data(spark)
    dataset.printSchema()

    model = train_pipeline(dataset)

    # Predecir para el Lunes (día 2 en Spark) a las 8 de la mañana (hora pico)
    predict_max_demand_zone(spark, model, target_day=2, target_hour=8)


    # Guardar el modelo usando las variables de entorno
    load_dotenv(find_dotenv())
    minio_bucket = os.getenv("MINIO_BUCKET")
    minio_groupPath = os.getenv("MINIO_GROUP_PATH")
    
    ruta_modelo_s3 = f"s3a://{minio_bucket}/{minio_groupPath}/modelos/rf_demanda_model"
    
    # Añadimos un bloque try-except para guardar el modelo de forma local si MinIO falla en el último paso
    try:
        print(f"Guardando modelo en MinIO: {ruta_modelo_s3}")
        model.write().overwrite().save(ruta_modelo_s3)
    except Exception as e:
        ruta_modelo_local = str(Path(__file__).resolve().parents[2] / "Entrega1_Pd2" / "datos" / "modelos" / "rf_demanda_model")
        print(f"Error al guardar en MinIO: {str(e).splitlines()[0]}")
        print(f"Hacemos fallback y guardamos el modelo localmente en: {ruta_modelo_local}")
        model.write().overwrite().save(ruta_modelo_local)

    spark.stop()