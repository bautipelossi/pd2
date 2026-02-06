import pandas as pd
import numpy as np
import os
from pyproj import Transformer

# --- CONFIGURACIÓN ---
# Sistema de coordenadas de origen (NYC Long Island ft) y destino (GPS Mundial)
# EPSG:2263 es el estándar para agencias de NYC. EPSG:4326 es lat/lon estándar.
CRS_ORIGEN = 'epsg:2263'
CRS_DESTINO = 'epsg:4326'


def cargar_datos(ruta_archivo):
    print(f"--- 1. Cargando datos desde: {ruta_archivo} ---")
    if not os.path.exists(ruta_archivo):
        print(f"❌ ERROR: No se encuentra el archivo.")
        return None
    try:
        # Cargamos solo columnas útiles para ahorrar memoria si el archivo es gigante
        # Si da error, quita el argumento 'usecols'
        df = pd.read_csv(ruta_archivo, low_memory=False)
        print(f"✅ Datos cargados. Filas iniciales: {len(df)}")
        return df
    except Exception as e:
        print(f"❌ Error al leer CSV: {e}")
        return None


def preprocesar_fechas(df):
    print("--- 2. Procesando fechas y filtrando (2023-2024) ---")

    # Filtrado inicial rápido
    if 'Yr' in df.columns:
        df = df[df['Yr'].isin([2023, 2024])].copy()

    # Mapeo y creación de timestamp
    cols_map = {'Yr': 'year', 'M': 'month', 'D': 'day', 'HH': 'hour', 'MM': 'minute'}
    try:
        temp = df[list(cols_map.keys())].rename(columns=cols_map)
        df['timestamp'] = pd.to_datetime(temp)
    except Exception as e:
        print(f"⚠️ Error creando fechas: {e}")
        return df

    # Ordenar cronológicamente
    df = df.sort_values('timestamp')
    print(f"✅ Registros tras filtro temporal: {len(df)}")
    return df


def convertir_geometria(df):
    """
    Convierte WKT State Plane (pies) a Latitud/Longitud real (GPS)
    para poder pintarlo en mapas interactivos.
    """
    print("--- 3. Extrayendo y convirtiendo coordenadas (Geodesia) ---")

    if 'WktGeom' not in df.columns:
        return df

    # 1. Extraer coordenadas crudas (X, Y en pies)
    # Regex busca: POINT (numero_x numero_y)
    coords = df['WktGeom'].str.extract(r'POINT \((?P<x>-?\d+\.?\d*)\s+(?P<y>-?\d+\.?\d*)\)')

    # Convertir a numérico
    x_coords = pd.to_numeric(coords['x'], errors='coerce')
    y_coords = pd.to_numeric(coords['y'], errors='coerce')

    # Eliminar filas sin coordenadas antes de transformar
    valid_idx = x_coords.notna() & y_coords.notna()
    x_valid = x_coords[valid_idx].values
    y_valid = y_coords[valid_idx].values

    try:
        # Crear transformador de coordenadas
        transformer = Transformer.from_crs(CRS_ORIGEN, CRS_DESTINO, always_xy=True)
        lon, lat = transformer.transform(x_valid, y_valid)

        # Asignar de vuelta al DF (inicializamos con NaN)
        df['longitude'] = np.nan
        df['latitude'] = np.nan
        df.loc[valid_idx, 'longitude'] = lon
        df.loc[valid_idx, 'latitude'] = lat

        print("✅ Coordenadas convertidas a GPS (Lat/Lon) exitosamente.")
    except Exception as e:
        print(f"⚠️ Error en conversión de coordenadas (quizás no instalaste pyproj): {e}")
        # Fallback: intentar usar las crudas si falla la conversión (aunque se verán mal)
        df.loc[valid_idx, 'longitude'] = x_valid
        df.loc[valid_idx, 'latitude'] = y_valid

    return df


def limpieza_y_features(df):
    print("--- 4. Limpieza y Feature Engineering para Visualización ---")

    # A. Limpieza Básica
    df = df.dropna(subset=['Vol', 'timestamp', 'latitude', 'longitude'])
    df = df[df['Vol'] >= 0]

    # Eliminar outliers extremos de tráfico (ej. error de sensor > 5000 coches en 15 min)
    limite_vol = df['Vol'].quantile(0.9995)
    df = df[df['Vol'] < limite_vol]

    # B. Textos Limpios (Para etiquetas en gráficos)
    cols_txt = ['Boro', 'street', 'fromSt', 'toSt', 'Direction']
    for c in cols_txt:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.title()  # "Queens" se ve mejor que "QUEENS"

    # C. Variables Temporales para Gráficos
    df['hora_entera'] = df['timestamp'].dt.hour
    df['dia_semana'] = df['timestamp'].dt.day_name()  # "Monday", "Tuesday"...
    df['mes_nombre'] = df['timestamp'].dt.month_name()

    # Variable categórica para filtros fáciles
    def get_momento_dia(h):
        if 6 <= h < 12:
            return 'Mañana'
        elif 12 <= h < 17:
            return 'Tarde'
        elif 17 <= h < 22:
            return 'Noche'
        else:
            return 'Madrugada'

    df['momento_dia'] = df['hora_entera'].apply(get_momento_dia)

    # D. Variables Numéricas para IA (One-Hot + Ciclos)
    # Ciclos horarios (útil para IA, no tanto para pintar mapas, pero lo dejamos)
    df['hour_sin'] = np.sin(2 * np.pi * df['hora_entera'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hora_entera'] / 24)

    # One-Hot Encoding optimizado (int8 para ahorrar memoria)
    if 'Boro' in df.columns:
        dummies = pd.get_dummies(df['Boro'], prefix='Boro', dtype='int8')
        df = pd.concat([df, dummies], axis=1)

    return df


def main():
    # --- RUTAS (AJUSTA ESTO) ---
    archivo_entrada = r"C:\Users\palon\Downloads\Automated_Traffic_Volume_Counts_20260122.csv"
    archivo_salida = r"C:\Users\palon\Downloads\dataset_trafico_vis_ready.csv"

    # 1. Carga
    df = cargar_datos(archivo_entrada)

    if df is not None:
        # 2. Fechas
        df = preprocesar_fechas(df)

        # 3. Geometría (CRUCIAL PARA MAPAS)
        df = convertir_geometria(df)

        # 4. Limpieza y Enriquecimiento
        df = limpieza_y_features(df)

        # 5. Selección de columnas final para mantener el archivo ligero
        cols_finales = [
            'timestamp', 'year', 'mes_nombre', 'dia_semana', 'hora_entera', 'momento_dia',
            'Vol', 'latitude', 'longitude',
            'Boro', 'street', 'fromSt', 'toSt', 'Direction', 'SegmentID',
            'hour_sin', 'hour_cos'
        ]
        # Agregar dinámicamente las columnas one-hot de Boro
        cols_finales += [c for c in df.columns if c.startswith('Boro_')]

        # Intersección para evitar errores si falta alguna columna
        cols_a_guardar = [c for c in cols_finales if c in df.columns]

        print(f"--- 5. Guardando {len(df)} registros optimizados en CSV ---")
        df[cols_a_guardar].to_csv(archivo_salida, index=False)
        print(f"¡Éxito! Archivo listo para visualización: {archivo_salida}")


if __name__ == "__main__":
    main()