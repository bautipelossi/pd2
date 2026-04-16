import os
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel

def create_spark_session():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = "C:/hadoop"
    
    spark = SparkSession.builder \
        .appName("Visualizacion_Demanda_Semanal") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def cargar_modelo(spark):
    ruta_modelo_local = str(Path(__file__).resolve().parents[2] / "datos" / "modelos" / "mejor_modelo_demanda")
    print(f"Cargando modelo desde: {ruta_modelo_local}")
    return PipelineModel.load(ruta_modelo_local)

def get_zone_dict():
    url_oficial = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    try:
        print("Descargando catálogo oficial de zonas...")
        df_zonas = pd.read_csv(url_oficial)
        return dict(zip(df_zonas['LocationID'], df_zonas['Zone']))
    except Exception as e:
        print(f"Aviso: No se pudo descargar el catálogo ({e})")
        return {}

def generar_datos_semanales(spark, model, diccionario_zonas):
    """Genera predicciones para los 7 días de la semana y filtra Top 5 de cada día"""
    print("Generando predicciones masivas para toda la semana (L-D)...")
    
    data_grid = []
    # 7 días * 24 horas * 265 zonas
    for dia in range(1, 8):
        for zona_id in range(1, 265):
            for hora in range(24):
                data_grid.append(Row(pulocationid=zona_id, day_of_week=dia, pickup_hour=hora))
            
    df_input = spark.createDataFrame(data_grid)
    predicciones = model.transform(df_input)
    
    # Convertimos a Pandas para la lógica de visualización
    df_pandas = predicciones.select("pulocationid", "day_of_week", "pickup_hour", "prediction").toPandas()
    df_pandas['nombre_zona'] = df_pandas['pulocationid'].map(lambda x: diccionario_zonas.get(x, f"Zona {x}"))
    df_pandas['prediction'] = df_pandas['prediction'].round(0)
    
    return df_pandas

def dibujar_dashboard_semanal(df_pandas):
    """Crea un gráfico interactivo con menú desplegable para cambiar de día"""
    
    dias_nombres = {1: "Domingo", 2: "Lunes", 3: "Martes", 4: "Miércoles", 5: "Jueves", 6: "Viernes", 7: "Sábado"}
    
    # Creamos la figura base (vacía)
    fig = go.Figure()
    
    # Lista de botones para el menú
    botones = []
    
    # Para cada día, añadimos sus 5 líneas (Top 5)
    for dia_idx in range(1, 8):
        df_dia = df_pandas[df_pandas['day_of_week'] == dia_idx]
        top_5_nombres = df_dia.groupby('nombre_zona')['prediction'].sum().nlargest(5).index.tolist()
        df_top = df_dia[df_dia['nombre_zona'].isin(top_5_nombres)]
        
        es_visible = (dia_idx == 2) 
        
        for zona in top_5_nombres:
            df_zona = df_top[df_top['nombre_zona'] == zona].sort_values('pickup_hour')
            fig.add_trace(go.Scatter(
                x=df_zona['pickup_hour'],
                y=df_zona['prediction'],
                name=zona,
                visible=es_visible,
                mode='lines+markers'
            ))
            
        visibilidad_dia = [False] * (7 * 5) 
        start_idx = (dia_idx - 1) * 5
        for i in range(start_idx, start_idx + 5):
            visibilidad_dia[i] = True
            
        botones.append(dict(
            label=dias_nombres[dia_idx],
            method="update",
            args=[{"visible": visibilidad_dia},
                  {"title.text": f"Top 5 Zonas con Más Demanda - {dias_nombres[dia_idx]}"}]
        ))

    # Configuración final del layout
    fig.update_layout(
        updatemenus=[dict(
            active=1, # Por defecto seleccionado Lunes
            buttons=botones,
            direction="down",
            pad={"r": 10, "t": 10},
            showactive=True,
            x=1.02,        
            xanchor="left",
            y=0.45,        
            yanchor="top"
        )],
        template="plotly_white",
        title=f"Top 5 Zonas con Más Demanda - Lunes",
        xaxis=dict(title="Hora del Día (0-23)", tickmode='linear', dtick=1),
        yaxis=dict(title="Viajes Esperados"),
        hovermode="x unified"
    )

    # Rutas de guardado
    base_dir = Path(__file__).resolve().parent
    ruta_final = base_dir.parent / "visualizacion" / "Prediccion_Demanda_E1a" / "dashboard_semanal_interactivo.html"
    ruta_final.parent.mkdir(parents=True, exist_ok=True)
    
    # ---> SOLUCIÓN DEFINITIVA A PRUEBA DE FALLOS <---
    # 1. Extraemos SOLO el "corazón" del gráfico (sin la etiqueta HTML que pone Plotly)
    grafico_div = fig.to_html(full_html=False, include_plotlyjs=True)
    
    # 2. Escribimos nosotros mismos el HTML con el título de la pestaña puesto a mano
    html_completo = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Top 5 Demanda Semanal NYC</title>
</head>
<body style="margin: 0; padding: 0;">
    {grafico_div}
</body>
</html>
"""
    
    # 3. Guardamos el archivo final (sobrescribiéndolo de forma limpia)
    with open(ruta_final, 'w', encoding='utf-8') as file:
        file.write(html_completo)

    print(f"\n¡Dashboard semanal generado con éxito!")
    print(f"Ruta: {ruta_final}")

if __name__ == "__main__":
    spark = create_spark_session()
    try:
        modelo = cargar_modelo(spark)
        dict_zonas = get_zone_dict()
        df_semanal = generar_datos_semanales(spark, modelo, dict_zonas)
        dibujar_dashboard_semanal(df_semanal)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()