import pandas as pd
from pathlib import Path

# Buscamos el archivo en la misma ruta que usamos antes
BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]
PARQUET_PATH = PROJECT_ROOT / "Entrega1_Pd2" / "datos" / "limpios" / "resumen_zona_hora.parquet"

def comprobar_datos():
    print(f"Buscando archivo en: {PARQUET_PATH}")
    
    if not PARQUET_PATH.exists():
        print(" ERROR: El archivo no existe. Revisa la ruta o vuelve a ejecutar agregaciones.py")
        return

    # Cargamos el dataframe
    df = pd.read_parquet(PARQUET_PATH)
    
    print("\n" + "="*50)
    print(" ARCHIVO CARGADO CON ÉXITO")
    print("="*50)
    
    # 1. Tamaño del dataset
    print(f"\n TAMAÑO DEL DATASET:")
    print(f"Filas: {df.shape[0]}")
    print(f"Columnas: {df.shape[1]}")
    
    # 2. Tipos de datos y nulos (Es crucial que no haya nulos y los tipos sean int/float)
    print("\n TIPOS DE DATOS Y VALORES NULOS:")
    info_df = pd.DataFrame({
        'Tipo': df.dtypes,
        'Nulos': df.isna().sum()
    })
    print(info_df)
    
    # 3. Muestra de las primeras 5 filas
    print("\n PRIMERAS 5 FILAS:")
    print(df.head().to_string())
    
    # 4. Comprobación de rangos (Sanity Check)
    print("\n CONTROL DE RANGOS (Sanity Check):")
    print(f"- Horas (pickup_hour): Min {df['pickup_hour'].min()} | Max {df['pickup_hour'].max()} (Debería ser 0 a 23)")
    print(f"- Días (day_of_week): Min {df['day_of_week'].min()} | Max {df['day_of_week'].max()} (Debería ser 1 a 7)")
    print(f"- Demanda mínima: {df['demanda_viajes'].min()} viajes (Debería ser >= 0)")
    print(f"- Demanda máxima: {df['demanda_viajes'].max()} viajes")
    print(f"- Total de zonas únicas (pulocationid): {df['pulocationid'].nunique()} (Deberían ser unas 260+)")

if __name__ == "__main__":
    comprobar_datos()