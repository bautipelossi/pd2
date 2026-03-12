import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import numpy as np
from dotenv import load_dotenv, find_dotenv


def load_data_from_minio():
    """Descarga el parquet agrupado desde MinIO directamente a Pandas usando .env"""
    # 1. Buscamos y cargamos el archivo .env que está en la raíz del proyecto
    load_dotenv(find_dotenv())

    # 2. Extraemos las variables de entorno
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    bucket = os.getenv("MINIO_BUCKET")
    group_path = os.getenv("MINIO_GROUP_PATH")

    # Validamos que se hayan cargado correctamente
    assert access_key, "Falta MINIO_ACCESS_KEY en el .env"
    assert secret_key, "Falta MINIO_SECRET_KEY en el .env"
    assert minio_endpoint, "Falta MINIO_ENDPOINT en el .env"
    assert bucket, "Falta MINIO_BUCKET en el .env"
    assert group_path, "Falta MINIO_GROUP_PATH en el .env"

    print("Conectando a MinIO y descargando datos...")
    
    # 3. Construimos la ruta dinámica a tu archivo
    ruta_s3 = f"s3://{bucket}/{group_path}/resumen_zona_hora.parquet"
    print(f"Leyendo: {ruta_s3}")

    # 4. Le pasamos las credenciales a Pandas a través de storage_options
    df = pd.read_parquet(
        ruta_s3,
        storage_options={
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {'endpoint_url': minio_endpoint}
        }
    )
    return df


def train_baseline(df):
    """Entrena un modelo base usando Scikit-Learn"""
    print("\nPreparando el pipeline de Scikit-Learn...")

    # Si no tienes 'day_of_week' pero tienes 'date_only', lo creamos:
    if 'day_of_week' not in df.columns and 'date_only' in df.columns:
        df['day_of_week'] = pd.to_datetime(df['date_only']).dt.dayofweek

    # Ajusta 'demanda_viajes' si en tu parquet agrupado la columna de conteo se llama distinto
    X = df[['pulocationid', 'pickup_hour', 'day_of_week']]
    y = df['demanda_viajes']  

    # Hacemos split 80/20
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Creamos un preprocesador para hacer One-Hot Encoding de las categóricas
    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', OneHotEncoder(handle_unknown='ignore'), ['pulocationid', 'day_of_week'])
        ],
        remainder='passthrough'  # Deja la hora como numérica
    )

    # Creamos el pipeline con el Árbol de Decisión
    baseline_model = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', DecisionTreeRegressor(max_depth=10, random_state=42))
    ])

    print("Entrenando Árbol de Decisión (Baseline)...")
    baseline_model.fit(X_train, y_train)

    # Evaluamos
    y_pred = baseline_model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)

    print("-" * 30)
    print(" RESULTADOS DEL BASELINE (Scikit-Learn)")
    print(f"RMSE: {rmse:.2f} viajes")
    print(f"MAE:  {mae:.2f} viajes de error medio")
    print("-" * 30)

    return baseline_model


if __name__ == "__main__":
    df_data = load_data_from_minio()
    print(f"Datos cargados: {df_data.shape[0]} filas.")

    model = train_baseline(df_data)