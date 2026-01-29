import pandas as pd
import numpy as np
import os


def cargar_datos(ruta_archivo):
    """
    Carga el dataset CSV desde la ruta específica.
    """
    print(f"--- Cargando datos desde: {ruta_archivo} ---")

    if not os.path.exists(ruta_archivo):
        print(f"ERROR CRÍTICO: No se encuentra el archivo en la ruta:\n{ruta_archivo}")
        return None

    try:
        df = pd.read_csv(ruta_archivo, low_memory=False)
        print(f"Datos cargados. Filas iniciales: {len(df)}")
        return df
    except Exception as e:
        print(f"Error al leer el CSV: {e}")
        return None


def construir_fecha_y_filtrar(df):
    """
    Une Yr, M, D, HH, MM y filtra solo 2023-2024.
    """
    print("--- Construyendo fechas y filtrando años 2023-2024 ---")

    # 1. Filtro rápido por año
    if 'Yr' in df.columns:
        df = df[df['Yr'].isin([2023, 2024])].copy()
    else:
        print("Error: No se encuentra la columna 'Yr'.")
        return df

    if len(df) == 0:
        print("ALERTA: El filtro devolvió 0 registros.")
        return df

    # 2. Construcción de datetime
    cols_fecha_map = {
        'Yr': 'year', 'M': 'month', 'D': 'day', 'HH': 'hour', 'MM': 'minute'
    }

    try:
        temp = df[list(cols_fecha_map.keys())].rename(columns=cols_fecha_map)
        df['timestamp'] = pd.to_datetime(temp)
    except KeyError as e:
        print(f"Faltan columnas necesarias para la fecha: {e}")

    print(f"Registros tras el filtro temporal: {len(df)}")
    return df


def limpieza_datos(df):
    """
    Elimina errores de volumen y TODOS los nulos.
    """
    print("--- Limpiando ruido y nulos ---")
    filas_antes = len(df)

    # 1. ELIMINAR NULOS (Nuevo paso solicitado)
    # dropna() elimina cualquier fila que tenga al menos un valor vacío
    df = df.dropna()
    print(f"Filas eliminadas por tener valores NULOS: {filas_antes - len(df)}")

    # Actualizamos el conteo para el siguiente paso
    filas_intermedias = len(df)

    # 2. Limpieza Lógica
    # El volumen (Vol) no puede ser negativo
    if 'Vol' in df.columns:
        df = df[df['Vol'] >= 0]

    # Normalizar texto a mayúsculas
    cols_texto = ['Boro', 'street', 'fromSt', 'toSt', 'Direction']
    for col in cols_texto:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()

    print(f"Filas eliminadas por lógica de negocio (Vol < 0): {filas_intermedias - len(df)}")
    return df


def feature_engineering(df):
    """
    Crea variables matemáticas útiles para la IA.
    """
    print("--- Generando variables para IA (Feature Engineering) ---")

    if 'timestamp' in df.columns:
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

        # Rush Hour (Hora Punta NYC)
        df['is_rush_hour'] = df['timestamp'].dt.hour.apply(
            lambda h: 1 if (7 <= h <= 10) or (16 <= h <= 20) else 0
        )

        # Transformación Cíclica
        df['hour_sin'] = np.sin(2 * np.pi * df['timestamp'].dt.hour / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['timestamp'].dt.hour / 24)

    return df


def main():
    # --- CONFIGURACIÓN DE RUTAS ---
    #archivo_entrada = r"C:\Users\palon\Downloads\Automated_Traffic_Volume_Counts_20260122.csv"
    #archivo_salida = r"C:\Users\palon\Downloads\dataset_trafico_limpio_2023_2024.csv"

    # 1. Carga
    df = cargar_datos(archivo_entrada)

    if df is not None:
        # 2. Fechas
        df = construir_fecha_y_filtrar(df)

        # 3. Limpieza (Incluye borrar nulos)
        df = limpieza_datos(df)

        # 4. Features
        df = feature_engineering(df)

        # 5. ORDENAR POR FECHA (Nuevo paso solicitado)
        print("--- Ordenando filas cronológicamente ---")
        df = df.sort_values(by='timestamp', ascending=True)

        # 6. Selección final y Guardado
        cols_finales = [
            'timestamp', 'Vol', 'Boro', 'street', 'fromSt', 'toSt', 'Direction',
            'SegmentID', 'day_of_week', 'is_weekend', 'is_rush_hour',
            'hour_sin', 'hour_cos'
        ]

        cols_a_guardar = [c for c in cols_finales if c in df.columns]

        print(f"--- Guardando resultado ({len(df)} registros) en: {archivo_salida} ---")
        df[cols_a_guardar].to_csv(archivo_salida, index=False)
        print("¡Proceso completado con éxito!")


if __name__ == "__main__":
    main()