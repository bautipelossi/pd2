import os
import sys
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel

# --- CONFIGURACIÓN DE PÁGINA STREAMLIT ---
st.set_page_config(page_title="NYC Taxi Simulator Pro", layout="wide", page_icon="🚕")

# --- FUNCIONES DE CACHÉ (Para no recargar Spark cada vez que tocas un botón) ---
@st.cache_resource
def iniciar_spark_y_modelo():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = "C:/hadoop"
    
    spark = SparkSession.builder.appName("NYC_Simulator").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # --- CORRECCIÓN AQUÍ: Usamos parents[1] para apuntar a src/modelos ---
    ruta_modelo = str(Path(__file__).resolve().parents[1] / "modelos" / "mejor_modelo_demanda")
    modelo = PipelineModel.load(ruta_modelo)
    
    # El parquet SÍ está en la raíz (datos/limpios/), así que este se queda con parents[2]
    ruta_parquet = str(Path(__file__).resolve().parents[2] / "datos" / "limpios" / "resumen_zona_hora.parquet")
    dataset = spark.read.parquet(ruta_parquet)
    
    # Extraer variables estáticas para que sea más rápido
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

# --- FUNCIÓN DE PREDICCIÓN EN TIEMPO REAL ---
def simular_demanda(spark, modelo, df_estatico, dia_semana, temp, lluvia, evento, dic_zonas):
    data_grid = [Row(
        pulocationid=int(z), 
        day_of_week=int(dia_semana), 
        pickup_hour=int(h),
        temperature_2m=float(temp), 
        precipitation=float(lluvia), 
        snowfall=0.0, 
        hay_evento=int(evento)
    ) for z in range(1, 265) for h in range(24)]
    
    df_grid = spark.createDataFrame(data_grid)
    df_input = df_grid.join(df_estatico, on="pulocationid", how="left").fillna(0)
    
    predicciones = modelo.transform(df_input)
    df_pd = predicciones.select("pulocationid", "pickup_hour", "prediction").toPandas()
    df_pd['nombre_zona'] = df_pd['pulocationid'].map(lambda x: dic_zonas.get(x, f"Zona {x}"))
    df_pd['prediction'] = df_pd['prediction'].round(0)
    return df_pd

# --- INTERFAZ WEB (UI) ---
def main():
    st.title("🚕 NYC Taxi Demand: Digital Twin Simulator")
    st.markdown("Simula cómo reacciona la ciudad ante diferentes condiciones climáticas y eventos culturales.")

    # Inicializar el motor
    with st.spinner("Cargando el motor de Inteligencia Artificial (Spark)..."):
        spark, modelo, df_estatico = iniciar_spark_y_modelo()
        dic_zonas = obtener_diccionario_zonas()

    # Barra lateral de controles
    st.sidebar.header("🎛️ Panel de Control")
    
    dias_map = {"Lunes": 2, "Martes": 3, "Miércoles": 4, "Jueves": 5, "Viernes": 6, "Sábado": 7, "Domingo": 1}
    dia_seleccionado = st.sidebar.selectbox("📅 Día de la semana", list(dias_map.keys()))
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("☁️ Condiciones Climáticas")
    temp = st.sidebar.slider("🌡️ Temperatura (ºC)", min_value=-15.0, max_value=40.0, value=15.0, step=0.5)
    lluvia = st.sidebar.slider("🌧️ Lluvia (mm/h)", min_value=0.0, max_value=30.0, value=0.0, step=1.0)
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("🎭 Agenda de la Ciudad")
    evento = st.sidebar.toggle("🎟️ Activar Gran Evento Público")
    
    # Calcular predicciones
    df_resultado = simular_demanda(spark, modelo, df_estatico, dias_map[dia_seleccionado], temp, lluvia, evento, dic_zonas)
    
    # Obtener el Top 5 del día simulado
    top_5_zonas = df_resultado.groupby('nombre_zona')['prediction'].sum().nlargest(5).index.tolist()
    df_top = df_resultado[df_resultado['nombre_zona'].isin(top_5_zonas)]

    # Dibujar gráfica
    fig = go.Figure()
    for zona in top_5_zonas:
        df_zona = df_top[df_top['nombre_zona'] == zona].sort_values('pickup_hour')
        fig.add_trace(go.Scatter(
            x=df_zona['pickup_hour'], y=df_zona['prediction'], 
            name=zona, mode='lines+markers', line=dict(width=3)
        ))

    fig.update_layout(
        template="plotly_dark",
        title=f"Curva de Demanda (Top 5 Zonas) - Simulación {dia_seleccionado}",
        xaxis=dict(title="Hora del Día (0-23)", tickmode='linear', dtick=1),
        yaxis=dict(title="Viajes Esperados (Predicción)"),
        hovermode="x unified",
        height=600
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Mostrar impacto matemático en pantalla
    st.info(f"**Análisis del Motor AI:** Evaluando {dia_seleccionado} con {temp}ºC, lluvia de {lluvia}mm y {'con evento' if evento else 'sin evento'}.")

if __name__ == "__main__":
    main()