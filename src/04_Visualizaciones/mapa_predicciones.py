import os
import sys
import json
import urllib.request
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel

def create_spark_session():
    """Inicia Spark solo para predicción"""
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = "C:/hadoop"
    
    spark = SparkSession.builder \
        .appName("Visualizacion_Mapa_Demanda") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def cargar_modelo(spark):
    """Carga el Pipeline completo"""
    ruta_modelo = str(Path(__file__).resolve().parents[2] / "Entrega1_Pd2" / "datos" / "modelos" / "mejor_modelo_demanda")
    print(f"Cargando modelo desde: {ruta_modelo}")
    return PipelineModel.load(ruta_modelo)

def obtener_nombres_zonas():
    """Descarga el catálogo oficial de nombres de zonas"""
    url_oficial = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    try:
        df_zonas = pd.read_csv(url_oficial)
        return dict(zip(df_zonas['LocationID'], df_zonas['Zone']))
    except Exception:
        return {}

def obtener_geometria_mapa():
    """Descarga el archivo GeoJSON oficial con las formas (polígonos) de NYC"""
    print("Descargando polígonos del mapa de NYC desde la web del gobierno...")
    # ---> CAMBIO 1: Nueva URL oficial (dataset 8meu-9t5y) <---
    url_geojson = "https://data.cityofnewyork.us/api/geospatial/8meu-9t5y?method=export&format=GeoJSON"
    
    # ---> CAMBIO 2: Añadimos User-Agent para disfrazarnos de navegador web <---
    req = urllib.request.Request(
        url_geojson, 
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    )
    respuesta = urllib.request.urlopen(req)
    return json.loads(respuesta.read())

def generar_datos_mapa(spark, model, dia_semana, diccionario_zonas):
    """Genera predicciones de todas las zonas para las 24 horas de un día específico"""
    print(f"Generando predicciones espaciales para el día {dia_semana}...")
    
    data_grid = []
    for zona_id in range(1, 265):
        for hora in range(24):
            data_grid.append(Row(pulocationid=zona_id, day_of_week=dia_semana, pickup_hour=hora))
            
    df_input = spark.createDataFrame(data_grid)
    predicciones = model.transform(df_input)
    
    df_pandas = predicciones.select("pulocationid", "pickup_hour", "prediction").toPandas()
    
    # Redondeamos viajes
    df_pandas['prediction'] = df_pandas['prediction'].round(0)
    # Añadimos el nombre de la zona para que salga al pasar el ratón
    df_pandas['nombre_zona'] = df_pandas['pulocationid'].map(lambda x: diccionario_zonas.get(x, f"Zona {x}"))
    
    # Plotly necesita que el ID sea texto (String) para cruzarlo con el mapa
    df_pandas['pulocationid_str'] = df_pandas['pulocationid'].astype(str)
    
    # Ordenamos por hora para que el slider del mapa vaya en orden (0 a 23)
    df_pandas = df_pandas.sort_values('pickup_hour')
    
    return df_pandas

def dibujar_mapa_interactivo(df_pandas, geojson_ny, dia_nombre):
    """Usa Plotly para crear un mapa de calor animado y lo guarda en HTML"""
    print("Generando mapa interactivo animado...")
    
    # Determinamos el límite de color para que sea constante durante todo el día
    max_viajes = df_pandas['prediction'].max()

    fig = px.choropleth_mapbox(
        df_pandas,
        geojson=geojson_ny,
        locations='pulocationid_str',
        # ---> CAMBIO 3: La API del gobierno lo llama "locationid" (sin guion bajo) <---
        featureidkey='properties.locationid', 
        color='prediction',                   
        animation_frame='pickup_hour',        
        hover_name='nombre_zona',             
        hover_data={'pulocationid_str': False, 'pickup_hour': False, 'prediction': True},
        color_continuous_scale="Viridis",     
        range_color=[0, max_viajes],          
        mapbox_style="carto-positron",        
        zoom=9.5,
        center={"lat": 40.73, "lon": -73.93}, 
        title=f"Mapa de Calor de Demanda de Taxis - {dia_nombre}"
    )

    fig.update_layout(
        margin={"r":0,"t":50,"l":0,"b":0},
        coloraxis_colorbar=dict(title="Viajes")
    )

    base_dir = Path(__file__).resolve().parent
    carpeta_ejercicio = base_dir.parent / "visualizacion" / "Prediccion_Demanda_E1a"
    carpeta_ejercicio.mkdir(parents=True, exist_ok=True)
    
    ruta_html = str(carpeta_ejercicio / f"mapa_demanda_{dia_nombre.lower()}.html")
    fig.write_html(ruta_html)
    
    print(f"\n¡Mapa animado generado con éxito!")
    print(f"Ruta: {ruta_html}")

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        DIA_A_ANALIZAR = 2 
        NOMBRE_DIA = "Lunes"
        
        modelo = cargar_modelo(spark)
        dict_zonas = obtener_nombres_zonas()
        
        mapa_geojson = obtener_geometria_mapa()
        
        df_mapa = generar_datos_mapa(spark, modelo, DIA_A_ANALIZAR, dict_zonas)
        
        dibujar_mapa_interactivo(df_mapa, mapa_geojson, NOMBRE_DIA)
        
    except Exception as e:
        print(f"Error al generar el mapa: {e}")
    finally:
        spark.stop()