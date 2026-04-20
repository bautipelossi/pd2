import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel

def create_spark_session():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = "C:/hadoop"
    spark = SparkSession.builder.appName("Exportador_Fase2").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def exportar_predicciones_base():
    spark = create_spark_session()
    print(" Iniciando exportación de demandas para Fase 2...")

    # 1. Cargar el modelo
    ruta_modelo = str(Path(__file__).resolve().parents[1] / "modelos" / "mejor_modelo_demanda")
    modelo = PipelineModel.load(ruta_modelo)

    # 2. Cargar datos estáticos (restaurantes, alquileres)
    ruta_parquet = str(Path(__file__).resolve().parents[2] / "datos" / "limpios" / "resumen_zona_hora.parquet")
    dataset_completo = spark.read.parquet(ruta_parquet)
    columnas_estaticas = ["pulocationid", "num_restaurantes", "precio_medio_rest", "num_alquileres", "precio_medio_alquiler"]
    df_estatico = dataset_completo.select(columnas_estaticas).dropDuplicates(["pulocationid"])

    # 3. Generar la matriz completa (7 días x 24 horas x 264 zonas = 44,352 filas)
    print(" Generando matriz de 44,352 combinaciones posibles...")
    data_grid = [Row(
        pulocationid=int(z), day_of_week=int(d), pickup_hour=int(h),
        temperature_2m=15.0, precipitation=0.0, snowfall=0.0, hay_evento=0 # Clima genérico base
    ) for z in range(1, 265) for d in range(1, 8) for h in range(24)]
    
    df_grid = spark.createDataFrame(data_grid)
    df_input = df_grid.join(df_estatico, on="pulocationid", how="left").fillna(0)

    # 4. Predecir
    print(" Calculando predicciones con el Random Forest...")
    predicciones = modelo.transform(df_input)
    
    # 5. Limpiar y guardar (Nos quedamos solo con lo necesario para cruzar luego)
    df_final = predicciones.select(
        "pulocationid", "day_of_week", "pickup_hour", "prediction"
    ).withColumnRenamed("prediction", "demanda_predicha")

    # Guardamos el Parquet en datos/limpios
    ruta_salida = str(Path(__file__).resolve().parents[2] / "datos" / "limpios" / "demandas_base_fase2.parquet")
    df_final.write.mode("overwrite").parquet(ruta_salida)
    
    print(f" ¡ÉXITO! Diccionario de demandas exportado a: {ruta_salida}")
    spark.stop()

if __name__ == "__main__":
    exportar_predicciones_base()