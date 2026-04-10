import os
import sys
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row

def create_spark_session():
    """Crea la sesión de Spark optimizada para Windows y MinIO"""
    load_dotenv(find_dotenv())
    
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = "C:/hadoop"
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
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def prepare_data(spark):
    """Carga los datos y asegura las columnas necesarias"""
    load_dotenv(find_dotenv())
    minio_bucket = os.getenv("MINIO_BUCKET")
    minio_groupPath = os.getenv("MINIO_GROUP_PATH")
    
    ruta_parquet = f"s3a://{minio_bucket}/{minio_groupPath}/limpios/resumen_zona_hora.parquet"
    project_root = Path(__file__).resolve().parents[2]
    ruta_local = project_root / "Entrega1_Pd2" / "datos" / "limpios" / "resumen_zona_hora.parquet"

    try:
        print(f"Intentando leer datos desde MinIO: {ruta_parquet}")
        df_grouped = spark.read.parquet(ruta_parquet)
        df_grouped.count()
        print("Datos cargados exitosamente desde MinIO.")
    except Exception as e:
        print(f"Fallo de conexión con MinIO: {str(e).splitlines()[0]}")
        print(f"Leyendo localmente desde: {ruta_local}")
        df_grouped = spark.read.parquet(str(ruta_local))

    if "day_of_week" not in df_grouped.columns and "date_only" in df_grouped.columns:
        df_grouped = df_grouped.withColumn("day_of_week", dayofweek("date_only"))

    return df_grouped.dropna()

def evaluate_model(model, dataset):
    """Calcula RMSE y MAE para un modelo dado sobre un dataset específico"""
    predictions = model.transform(dataset)
    eval_rmse = RegressionEvaluator(labelCol="demanda_viajes", predictionCol="prediction", metricName="rmse")
    eval_mae = RegressionEvaluator(labelCol="demanda_viajes", predictionCol="prediction", metricName="mae")
    
    return eval_rmse.evaluate(predictions), eval_mae.evaluate(predictions)

def train_and_compare(train_data, val_data):
    """Entrena RF y GBT en Train, los evalúa en Validation y devuelve el ganador"""
    # 1. Preparación del Pipeline (Común)
    indexer_zone = StringIndexer(inputCol="pulocationid", outputCol="zone_idx", handleInvalid="keep")
    indexer_day = StringIndexer(inputCol="day_of_week", outputCol="day_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["zone_idx", "day_idx"], outputCols=["zone_vec", "day_vec"])
    assembler = VectorAssembler(inputCols=["pickup_hour", "zone_vec", "day_vec"], outputCol="features")

    # 2. Entrenar y validar Random Forest
    rf = RandomForestRegressor(featuresCol="features", labelCol="demanda_viajes", numTrees=50, maxDepth=10)
    pipeline_rf = Pipeline(stages=[indexer_zone, indexer_day, encoder, assembler, rf])
    print("\n> Entrenando Random Forest (sobre set de Train)...")
    model_rf = pipeline_rf.fit(train_data)
    rmse_rf, mae_rf = evaluate_model(model_rf, val_data)

    # 3. Entrenar y validar Gradient-Boosted Trees
    gbt = GBTRegressor(featuresCol="features", labelCol="demanda_viajes", maxIter=20, maxDepth=5)
    pipeline_gbt = Pipeline(stages=[indexer_zone, indexer_day, encoder, assembler, gbt])
    print("> Entrenando Gradient-Boosted Trees (sobre set de Train)...")
    model_gbt = pipeline_gbt.fit(train_data)
    rmse_gbt, mae_gbt = evaluate_model(model_gbt, val_data)

    # 4. Mostrar comparativa
    print("\n" + "="*40)
    print(f" RESULTADOS EN VALIDATION (Selección)")
    print(f" RF  -> RMSE: {rmse_rf:.2f} | MAE: {mae_rf:.2f}")
    print(f" GBT -> RMSE: {rmse_gbt:.2f} | MAE: {mae_gbt:.2f}")
    print("="*40)

    if rmse_rf < rmse_gbt:
        print("GANADOR: Random Forest")
        return model_rf
    else:
        print("GANADOR: Gradient-Boosted Trees")
        return model_gbt

def predict_max_demand_zone(spark, model, target_day, target_hour):
    """Predice y muestra la zona con mayor demanda"""
    print(f"\n--- Prediciendo demanda para el Día {target_day} a las {target_hour}:00 ---")
    zonas_ids = range(1, 264)
    data_grid = [Row(pulocationid=int(z), day_of_week=int(target_day), pickup_hour=int(target_hour)) for z in zonas_ids]
    
    df_pred_input = spark.createDataFrame(data_grid)
    predicciones = model.transform(df_pred_input)
    top_zona = predicciones.orderBy(col("prediction").desc()).first()

    if top_zona:
        print(f"LA ZONA RECOMENDADA ES: {int(top_zona['pulocationid'])}")
        print(f"Viajes esperados (predicción): {round(top_zona['prediction'], 2)}")

if __name__ == "__main__":
    spark = create_spark_session()
    
    print("Preparando y dividiendo datos...")
    dataset = prepare_data(spark)
    
    # DIVISIÓN: Train (70%), Validation (15%), Test (15%)
    train_df, val_df, test_df = dataset.randomSplit([0.7, 0.15, 0.15], seed=42)

    # 1. Entrenar y elegir el mejor modelo usando Validation
    best_model = train_and_compare(train_df, val_df)

    # 2. Examen Final: Evaluar el ganador en Test (Datos completamente nuevos)
    print("\n" + "*"*40)
    print(" EXAMEN FINAL EN SET DE TEST")
    rmse_test, mae_test = evaluate_model(best_model, test_df)
    print(f" Rendimiento real -> RMSE: {rmse_test:.2f} | MAE: {mae_test:.2f}")
    print("*"*40)

    # 3. Predicción práctica
    predict_max_demand_zone(spark, best_model, target_day=2, target_hour=8)

    print("\n" + "-"*50)
    print("PROCESO DE CÁLCULO FINALIZADO EXITOSAMENTE")
    print("-" * 50)

    # 4. Guardar modelo
    try:
        minio_bucket = os.getenv("MINIO_BUCKET")
        minio_groupPath = os.getenv("MINIO_GROUP_PATH")
        ruta_modelo_s3 = f"s3a://{minio_bucket}/{minio_groupPath}/modelos/mejor_modelo_demanda"
        print(f"Guardando mejor modelo en la nube...")
        best_model.write().overwrite().save(ruta_modelo_s3)
    except Exception as e:
        ruta_modelo_local = str(Path(__file__).resolve().parents[2] / "Entrega1_Pd2" / "datos" / "modelos" / "mejor_modelo_demanda")
        print(f"Nota: Fallo en nube, guardando localmente en: {ruta_modelo_local}")
        best_model.write().overwrite().save(ruta_modelo_local)

    spark.stop()