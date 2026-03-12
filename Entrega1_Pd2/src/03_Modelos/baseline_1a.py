import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import numpy as np


def load_data_from_minio():
    """Descarga el parquet agrupado desde MinIO directamente a Pandas"""
    # Configuramos las credenciales de S3 para Pandas/s3fs
    os.environ['AWS_ACCESS_KEY_ID'] = '2FUJr4T13QnYp5fbhAUP'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'PdBhpHpYPjr8ZIParnrlFsIQApR8U5ao3VTT2dR7'
    os.environ['AWS_S3_ENDPOINT'] = 'https://minio.fdi.ucm.es'

    print("Conectando a MinIO y descargando datos...")
    # s3:// es el protocolo que entiende Pandas para MinIO
    # Apuntamos a vuestra carpeta taxomanos
    df = pd.read_parquet('s3://pd2/taxomanos/resumen_zona_hora.parquet',
                         storage_options={
                             "key": os.environ['AWS_ACCESS_KEY_ID'],
                             "secret": os.environ['AWS_SECRET_ACCESS_KEY'],
                             "client_kwargs": {'endpoint_url': os.environ['AWS_S3_ENDPOINT']}
                         })
    return df


def train_baseline(df):
    """Entrena un modelo base usando Scikit-Learn"""
    print("\nPreparando el pipeline de Scikit-Learn...")

    # Supongamos que tu DataFrame tiene 'pulocationid', 'pickup_hour', 'day_of_week'
    # y la variable a predecir 'demanda_viajes' (ajusta los nombres si son distintos)

    # Si no tienes 'day_of_week' pero tienes 'date_only', lo creamos:
    if 'day_of_week' not in df.columns and 'date_only' in df.columns:
        df['day_of_week'] = pd.to_datetime(df['date_only']).dt.dayofweek

    X = df[['pulocationid', 'pickup_hour', 'day_of_week']]
    y = df['demanda_viajes']  # Ajusta este nombre a como se llame en tu parquet

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