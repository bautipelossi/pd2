import pandas as pd
import plotly.express as px
import folium
from folium.plugins import HeatMap
import os

# --- CONFIGURACIÓN ---
ARCHIVO_ENTRADA = r"C:\Users\palon\Downloads\dataset_trafico_vis_ready.csv"
CARPETA_SALIDA = r"C:\Users\palon\Downloads\Reporte_Trafico_NYC"

if not os.path.exists(CARPETA_SALIDA):
    os.makedirs(CARPETA_SALIDA)


def cargar_datos(ruta):
    print("--- Cargando dataset limpio ---")
    # Cargamos solo columnas necesarias para optimizar memoria
    cols = ['latitude', 'longitude', 'Vol', 'hora_entera', 'Boro', 'street', 'SegmentID']
    try:
        df = pd.read_csv(ruta, usecols=cols)
        print(f"Datos cargados: {len(df)} registros.")
        return df
    except Exception as e:
        print(f"Error cargando datos: {e}")
        return None


def generar_mapa_animado(df):
    """
    Crea un mapa interactivo (Plotly) que muestra la evolución del tráfico por hora.
    Agrupa los datos para mostrar un 'Día Promedio'.
    """
    print("--- Generando 1. Mapa Animado de Tráfico (Plotly) ---")

    # 1. Agregamos datos: Promedio de volumen por Segmento y Hora
    # Esto reduce millones de filas a unas pocas miles para que el navegador no explote
    df_agg = df.groupby(['SegmentID', 'hora_entera', 'street', 'Boro', 'latitude', 'longitude'])[
        'Vol'].mean().reset_index()

    # Redondeamos volumen para que se vea limpio
    df_agg['Vol'] = df_agg['Vol'].round(0)

    # Ordenamos por hora para que la animación fluya bien
    df_agg = df_agg.sort_values('hora_entera')

    # 2. Configurar el Mapa
    fig = px.scatter_mapbox(
        df_agg,
        lat="latitude",
        lon="longitude",
        color="Vol",  # El color depende del volumen
        size="Vol",  # El tamaño del círculo también
        hover_name="street",  # Al pasar el ratón muestra la calle
        hover_data={"Boro": True, "Vol": True, "latitude": False, "longitude": False, "SegmentID": False},
        animation_frame="hora_entera",  # ¡Aquí está la magia! Barra de tiempo
        color_continuous_scale=px.colors.sequential.Plasma,  # Colores tipo fuego/neón
        size_max=15,  # Tamaño máximo de los puntos
        zoom=10,
        center={"lat": 40.73, "lon": -73.93},  # Centro de NYC
        mapbox_style="carto-positron",  # Estilo de mapa limpio (gratis, no requiere token)
        title="Evolución del Tráfico Promedio por Hora en NYC"
    )

    archivo = os.path.join(CARPETA_SALIDA, "1_mapa_animado_trafico.html")
    fig.write_html(archivo)
    print(f"✅ Mapa animado guardado en: {archivo}")


def generar_mapa_calor(df):
    """
    Crea un mapa estático de calor (Folium) para identificar 'Hotspots'.
    """
    print("--- Generando 2. Mapa de Calor General (Folium) ---")

    # Filtramos: Nos interesan los puntos donde REALMENTE hay tráfico
    # Tomamos solo registros con volumen considerable para no ensuciar el mapa
    umbral = df['Vol'].quantile(0.50)  # Solo el 50% superior de tráfico
    df_calor = df[df['Vol'] > umbral].copy()

    # Agrupamos por ubicación para obtener la intensidad promedio total
    data_heatmap = df_calor.groupby(['latitude', 'longitude'])['Vol'].mean().reset_index()

    # Lista de listas [lat, lon, peso] requerida por Folium
    heat_data = data_heatmap[['latitude', 'longitude', 'Vol']].values.tolist()

    # Crear mapa base
    m = folium.Map(location=[40.73, -73.93], zoom_start=11, tiles="CartoDB positron")

    # Añadir capa de calor
    HeatMap(heat_data, radius=10, blur=15, max_zoom=13).add_to(m)

    archivo = os.path.join(CARPETA_SALIDA, "2_mapa_calor_zonas.html")
    m.save(archivo)
    print(f"✅ Mapa de calor guardado en: {archivo}")


def generar_grafico_lineas(df):
    """
    Gráfico de líneas comparativo: Hora vs Volumen por Distrito (Boro).
    """
    print("--- Generando 3. Comparativa de Distritos (Plotly) ---")

    # Agrupar por Hora y Distrito
    df_b = df.groupby(['hora_entera', 'Boro'])['Vol'].mean().reset_index()

    fig = px.line(
        df_b,
        x='hora_entera',
        y='Vol',
        color='Boro',
        markers=True,
        title="Perfil Diario de Tráfico por Distrito (Volumen Promedio)",
        labels={'hora_entera': 'Hora del Día (0-23)', 'Vol': 'Volumen Promedio de Vehículos', 'Boro': 'Distrito'}
    )

    fig.update_layout(xaxis=dict(tickmode='linear', dtick=1))  # Mostrar todas las horas en eje X

    archivo = os.path.join(CARPETA_SALIDA, "3_grafico_horarios_distritos.html")
    fig.write_html(archivo)
    print(f"✅ Gráfico de líneas guardado en: {archivo}")


def main():
    df = cargar_datos(ARCHIVO_ENTRADA)

    if df is not None:
        # Nota: Si el dataset es GIGANTE (millones de filas), toma una muestra para probar primero:
        # df = df.sample(100000)

        generar_mapa_animado(df)
        generar_mapa_calor(df)
        generar_grafico_lineas(df)

        print("\n--- ¡PROCESO FINALIZADO! ---")
        print(f"Abre la carpeta '{CARPETA_SALIDA}' y haz doble clic en los archivos HTML.")


if __name__ == "__main__":
    main()