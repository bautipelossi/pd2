import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, count, date_trunc
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row



def create_spark_session():
    """Crea la sesión de Spark configurada para leer desde MinIO (S3-compatible)"""
    # IMPORTANTE: Cambia estas variables por las que indique tu MinIO.pdf
    minio_endpoint = "https://minio.fdi.ucm.es"
    access_key = "2FUJr4T13QnYp5fbhAUP"
    secret_key = "PdBhpHpYPjr8ZIParnrlFsIQApR8U5ao3VTT2dR7"

    spark = SparkSession.builder \
        .appName("Prediccion_Demanda_Taxi_Ex1a") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
        .getOrCreate()

    return spark


def predict_max_demand_zone(spark, model, target_day, target_hour):
    """
    Dada una hora y un día de la semana, predice qué zona tendrá más demanda.
    target_day: int (ej. 1 para Domingo, 2 para Lunes... según codificación de Spark)
    target_hour: int (0 a 23)
    """
    print(f"\n--- Prediciendo demanda para el Día {target_day} a las {target_hour}:00 ---")

    # 1. Generamos un DataFrame "falso" con todas las zonas posibles (IDs del 1 al 263 aprox)
    # y fijamos el día y la hora que nos interesan.
    zonas_ids = range(1, 264)
    data_grid = [Row(pulocationid=float(z), day_of_week=int(target_day), pickup_hour=int(target_hour)) for z in
                 zonas_ids]

    df_pred_input = spark.createDataFrame(data_grid)

    # 2. Pasamos este grid por el modelo entrenado
    predicciones = model.transform(df_pred_input)

    # 3. Ordenamos de mayor a menor predicción y sacamos la zona TOP
    top_zona = predicciones.orderBy(col("prediction").desc()).first()

    if top_zona:
        print(f" LA ZONA RECOMENDADA ES: {int(top_zona['pulocationid'])}")
        print(f" Viajes esperados (predicción): {round(top_zona['prediction'], 2)}")
    else:
        print("No se pudo realizar la predicción.")


def prepare_data(spark):
    """Carga los datos ya agrupados por zona y hora directamente desde MinIO"""
    # 1. Leer directamente el resumen (Cambia 'nombre_real_del_bucket')
    # Le indicamos el bucket 'pd2' y la carpeta 'taxomanos'
    df_grouped = spark.read.parquet("s3a://pd2/taxomanos/resumen_zona_hora.parquet")

    # Asegúrate de que los nombres de las columnas coinciden con lo que espera el Pipeline.
    # Si tu parquet agrupado tiene columnas diferentes, renómbralas aquí.
    # Por ejemplo, imaginemos que tu columna de conteo se llama 'conteo_viajes':
    # df_grouped = df_grouped.withColumnRenamed("conteo_viajes", "demanda_viajes")

    # 2. Si el parquet no tiene el día de la semana explícito, lo sacas de la fecha
    # (Comenta esto si tu resumen_zona_hora ya tiene una columna 'day_of_week')
    if "day_of_week" not in df_grouped.columns and "date_only" in df_grouped.columns:
        df_grouped = df_grouped.withColumn("day_of_week", dayofweek("date_only"))

    # Aquí harías el .join() con el clima si lo necesitáis

    return df_grouped.dropna()


def train_pipeline(df):
    """Crea el pipeline y entrena el modelo de regresión"""

    # 1. Convertir variables categóricas (Zona y Día de la semana)
    # StringIndexer convierte el ID a un índice numérico que Spark entiende
    indexer_zone = StringIndexer(inputCol="pulocationid", outputCol="zone_idx", handleInvalid="keep")
    indexer_day = StringIndexer(inputCol="day_of_week", outputCol="day_idx", handleInvalid="keep")

    # (Opcional) OneHotEncoder mejora el modelo para categóricas sin orden jerárquico
    encoder = OneHotEncoder(inputCols=["zone_idx", "day_idx"], outputCols=["zone_vec", "day_vec"])

    # 2. Ensamblar todas las características en un único vector 'features'
    # Si añades clima (ej. temperatura), añádelo a esta lista
    assembler = VectorAssembler(inputCols=["pickup_hour", "zone_vec", "day_vec"], outputCol="features")

    # 3. Definir el modelo (Random Forest es un gran baseline)
    rf = RandomForestRegressor(featuresCol="features", labelCol="demanda_viajes", numTrees=50, maxDepth=10)

    # 4. Construir el Pipeline
    pipeline = Pipeline(stages=[indexer_zone, indexer_day, encoder, assembler, rf])

    # 5. Split Train/Test (80% entrenamiento, 20% validación)
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    print("Entrenando el modelo RandomForest en el clúster...")
    model = pipeline.fit(train_data)

    # 6. Evaluación básica
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="demanda_viajes", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"Error Cuadrático Medio (RMSE): {rmse} viajes por zona/hora")

    return model


if __name__ == "__main__":
    spark = create_spark_session()

    print("Preparando datos...")
    dataset = prepare_data(spark)

    # Enseña el esquema resultante
    dataset.printSchema()

    model = train_pipeline(dataset)

    # Vamos a probar a predecir para el Lunes (día 2 en Spark) a las 8 de la mañana (hora pico)
    predict_max_demand_zone(spark, model, target_day=2, target_hour=8)

    # Guardar el modelo entrenado en MinIO para que la app (Streamlit) pueda consumirlo después
    model.write().overwrite().save("s3a://vuestro-bucket/modelos/rf_demanda_model")

    spark.stop()