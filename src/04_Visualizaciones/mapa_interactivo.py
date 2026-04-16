import os
import sys
import json
import urllib.request
import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel

# --- CONFIGURACIÓN DE PÁGINA STREAMLIT ---
st.set_page_config(page_title="NYC Taxi Map Simulator", layout="wide", page_icon="🗺️")

# --- FUNCIONES DE CACHÉ (Evitan recargar todo al mover un deslizador) ---
@st.cache_resource
def iniciar_spark_y_modelo():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = "C:/hadoop"
    
    spark = SparkSession.builder.appName("NYC_Map_Simulator").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Rutas corregidas hacia la raíz del proyecto
    ruta_modelo = str(Path(__file__).resolve().parents[1] / "modelos" / "mejor_modelo_demanda")
    modelo = PipelineModel.load(ruta_modelo)
    
    ruta_parquet = str(Path(__file__).resolve().parents[2] / "datos" / "limpios" / "resumen_zona_hora.parquet")
    dataset = spark.read.parquet(ruta_parquet)
    
    columnas_estaticas = ["pulocationid", "num_restaurantes", "precio_medio_rest", "num_alquileres", "precio_medio_alquiler"]
    df_estatico = dataset.select(columnas_estaticas).dropDuplicates(["pulocationid"])
    
    return spark, modelo, df_estatico

@st.cache_data
def obtener_diccionario_zonas():
    url_oficial = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    try:
        df_zonas = pd.read_csv(url_oficial)
        return dict(zip(df_zonas['LocationID'], df_zonas['Zone']))
    except:
        return {}

@st.cache_data
def obtener_geometria_mapa():
    # Se descarga solo la primera vez y se guarda en memoria
    url_geojson = "https://data.cityofnewyork.us/api/geospatial/8meu-9t5y?method=export&format=GeoJSON"
    req = urllib.request.Request(url_geojson, headers={'User-Agent': 'Mozilla/5.0'})
    return json.loads(urllib.request.urlopen(req).read())

# --- CÁLCULO DE PREDICCIONES EN TIEMPO REAL ---
def simular_demanda_mapa(spark, modelo, df_estatico, dia_semana, temp, lluvia, evento, dic_zonas):
    # Generamos las 24 horas para poder animar el mapa
    data_grid = [Row(
        pulocationid=int(z), day_of_week=int(dia_semana), pickup_hour=int(h),
        temperature_2m=float(temp), precipitation=float(lluvia), snowfall=0.0, hay_evento=int(evento)
    ) for z in range(1, 265) for h in range(24)]
    
    df_grid = spark.createDataFrame(data_grid)
    df_input = df_grid.join(df_estatico, on="pulocationid", how="left").fillna(0)
    
    predicciones = modelo.transform(df_input)
    df_pd = predicciones.select("pulocationid", "pickup_hour", "prediction").toPandas()
    df_pd['nombre_zona'] = df_pd['pulocationid'].map(lambda x: dic_zonas.get(x, f"Zona {x}"))
    df_pd['pulocationid_str'] = df_pd['pulocationid'].astype(str)
    
    # SEGURIDAD: Evitar predicciones negativas que estropean la escala de color de 0
    df_pd['prediction'] = df_pd['prediction'].apply(lambda x: max(0, x)).round(0)
    
    return df_pd.sort_values('pickup_hour')

# --- INTERFAZ WEB ---
def main():
    st.title("🗺️ Mapa Interactivo: NYC Taxi Digital Twin")
    st.markdown("Observa cómo evoluciona geográficamente la demanda de taxis al cambiar el clima y los eventos. Dale al botón **Play** debajo del mapa para ver la evolución horaria.")

    with st.spinner("Arrancando motores y descargando satélite de NYC (Paciencia la primera vez)..."):
        spark, modelo, df_estatico = iniciar_spark_y_modelo()
        dic_zonas = obtener_diccionario_zonas()
        mapa_geojson = obtener_geometria_mapa()

    # Controles en la barra lateral
    st.sidebar.header("🎛️ Controles del Mapa")
    
    dias_map = {"Lunes": 2, "Martes": 3, "Miércoles": 4, "Jueves": 5, "Viernes": 6, "Sábado": 7, "Domingo": 1}
    dia_seleccionado = st.sidebar.selectbox("📅 Día de la semana", list(dias_map.keys()))
    
    st.sidebar.markdown("---")
    temp = st.sidebar.slider("🌡️ Temperatura (ºC)", min_value=-15.0, max_value=40.0, value=15.0, step=0.5)
    lluvia = st.sidebar.slider("🌧️ Lluvia (mm/h)", min_value=0.0, max_value=30.0, value=0.0, step=1.0)
    evento = st.sidebar.toggle("🎟️ Activar Gran Evento")
    
    # Calculamos la demanda con los parámetros elegidos
    df_mapa = simular_demanda_mapa(spark, modelo, df_estatico, dias_map[dia_seleccionado], temp, lluvia, evento, dic_zonas)
    
    # Fijamos el máximo de la leyenda para que no salte al cambiar de hora
    max_viajes = df_mapa['prediction'].max()

    # --- ESCALA DE COLORES COMPUESTA PERSONALIZADA ---
    escala_personalizada = [
        [0.0, "rgb(40, 40, 40)"],    # 0%: Gris oscuro / Casi negro
        [0.05, "rgb(120, 120, 120)"],# 5%: Gris medio
        [0.1, "#4b0c6b"],            # 10%: Morado oscuro (Comienza la demanda)
        [0.3, "#781c6d"],            # 30%: Magenta
        [0.5, "#a52c60"],            # 50%: Rojo
        [0.7, "#ed6925"],            # 70%: Naranja
        [1.0, "#fcffa4"]             # 100%: Amarillo brillante
    ]

    # Generamos el mapa animado de Plotly
    fig = px.choropleth_mapbox(
        df_mapa, geojson=mapa_geojson, locations='pulocationid_str', featureidkey='properties.locationid', 
        color='prediction', animation_frame='pickup_hour', hover_name='nombre_zona', 
        hover_data={'pulocationid_str': False, 'pickup_hour': False, 'prediction': True},
        color_continuous_scale=escala_personalizada, range_color=[0, max_viajes],
        mapbox_style="carto-darkmatter", zoom=9.5, center={"lat": 40.7128, "lon": -73.93} 
    )

    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        height=700, # Hacemos el mapa bien grande
        coloraxis_colorbar=dict(
            title="Viajes", thicknessmode="pixels", thickness=15, 
            yanchor="top", y=0.9, ticks="outside"
        )
    )
    
    # Lo incrustamos en la web de Streamlit
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()