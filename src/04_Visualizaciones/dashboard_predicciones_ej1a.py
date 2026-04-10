import os
import sys
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from pyspark.sql import SparkSession, Row
from pyspark.ml.regression import GBTRegressionModel

def create_spark_session():
    """Inicia Spark solo para predicción"""
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = "C:/hadoop"
    
    spark = SparkSession.builder \
        .appName("Visualizacion_Demanda") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def cargar_modelo(spark):
    """Carga el modelo ganador que guardamos previamente"""
    ruta_modelo_local = str(Path(__file__).resolve().parents[2] / "Entrega1_Pd2" / "datos" / "modelos" / "mejor_modelo_demanda")
    print(f"Cargando modelo desde: {ruta_modelo_local}")
    return GBTRegressionModel.load(ruta_modelo_local)

def generar_datos_grafica(spark, model, dia_semana):
    """Genera predicciones para todas las horas en zonas clave"""
    
    # Zonas de alto interés para visualizar
    zonas_interes = {
        132: "Aeropuerto JFK",
        138: "LaGuardia (LGA)",
        237: "Upper East Side South",
        161: "Midtown Center",
        230: "Times Square"
    }
    
    print(f"Generando predicciones para el día {dia_semana} en las 24 horas...")
    data_grid = []
    
    # Creamos una cuadrícula: Cada zona combinada con cada hora del día (0-23)
    for zona_id, nombre in zonas_interes.items():
        for hora in range(24):
            data_grid.append(Row(
                pulocationid=zona_id, 
                nombre_zona=nombre, 
                day_of_week=dia_semana, 
                pickup_hour=hora
            ))
            
    df_input = spark.createDataFrame(data_grid)
    
    # Hacemos la predicción masiva
    predicciones = model.transform(df_input)
    
    # Convertimos a Pandas para poder dibujarlo con Plotly
    df_pandas = predicciones.select("nombre_zona", "pickup_hour", "prediction").toPandas()
    
    # Redondeamos los viajes para que el gráfico quede limpio
    df_pandas['prediction'] = df_pandas['prediction'].round(0)
    
    return df_pandas

def dibujar_grafica_interactiva(df_pandas):
    """Usa Plotly para crear un HTML interactivo y lo guarda en la carpeta correspondiente"""
    
    fig = px.line(
        df_pandas, 
        x="pickup_hour", 
        y="prediction", 
        color="nombre_zona",
        title="Curva de Demanda de Taxis en NYC - Lunes (Predicción GBT)",
        labels={
            "pickup_hour": "Hora del Día (0-23)",
            "prediction": "Viajes Esperados",
            "nombre_zona": "Zona de NYC"
        },
        markers=True # Pone un puntito en cada hora
    )
    
    # Mejoramos el diseño (estética profesional)
    fig.update_layout(
        template="plotly_white",
        hovermode="x unified", # Al pasar el ratón muestra los datos de todas las líneas a la vez
        xaxis=dict(tickmode='linear', tick0=0, dtick=1), # Fuerza a mostrar todas las horas
        legend_title_text='Zonas Analizadas'
    )
    
    # --- Lógica de rutas para guardar en la carpeta correcta ---
    # Asumiendo que el script está en src/04_Visualizacion/
    base_dir = Path(__file__).resolve().parent
    
    # Buscamos la carpeta 'visualizacion' que está al mismo nivel que '04_Visualizacion'
    carpeta_visualizacion = base_dir.parent / "visualizacion"
    
    # Creamos la subcarpeta específica para este ejercicio
    carpeta_ejercicio = carpeta_visualizacion / "E1a_Prediccion_Demanda"
    
    # Nos aseguramos de que la carpeta existe
    carpeta_ejercicio.mkdir(parents=True, exist_ok=True)
    
    # Definimos la ruta final del archivo HTML
    ruta_html = str(carpeta_ejercicio / "dashboard_demanda_nyc_lunes.html")
    
    # Guardamos el gráfico
    fig.write_html(ruta_html)
    print(f"\n¡Gráfica generada con éxito!")
    print(f"Ruta: {ruta_html}")

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        # 1. Cargamos el modelo ya entrenado (rápido, sin re-entrenar)
        modelo = cargar_modelo(spark)
        
        # 2. Predicciones para un Lunes (día 2)
        df_plot = generar_datos_grafica(spark, modelo, dia_semana=2)
        
        # 3. Dibujamos y guardamos
        dibujar_grafica_interactiva(df_plot)
        
    except Exception as e:
        print(f"Error al generar visualización: {e}")
        
    finally:
        spark.stop()