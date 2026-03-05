from minio import Minio
import os
from dotenv import load_dotenv
import io
import urllib3
import pandas as pd
import geopandas as gpd
from pathlib import Path

def crear_cliente_minio() -> Minio:
    """ Inicializa un cliente MinIO a partir de las variables de entorno en .env
    (MINIO_ACCESS_KEY, MINIO_SECRET_KEY y MINIO_ENDPOINT).

    Returns:
        Minio: Cliente de MinIO
    """
    load_dotenv()
    minio_access_key=os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key=os.getenv("MINIO_SECRET_KEY")
    minio_endpoint=os.getenv("MINIO_ENDPOINT")

    assert minio_access_key, "Falta MINIO_ACCESS_KEY en el entorno/.env"
    assert minio_secret_key, "Falta MINIO_SECRET_KEY en el entorno/.env"
    assert minio_endpoint, "Falta MINIO_ENDPOINT en el entorno/.env"

    cliente_http = urllib3.PoolManager(
    
        timeout=urllib3.Timeout(connect=10.0, read=600.0), 
        
        retries=urllib3.Retry(
            total=5, 
            backoff_factor=0.5, 
            status_forcelist=[500, 502, 503, 504]
        )
    )

    return Minio(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=True,
        http_client = cliente_http
    )


def subir_minio(df: pd.DataFrame, client:Minio, path: str, minio_object: str) -> None:
    """Sube dataframe a MinIO, convirtiéndolo a parquet en el proceso.

    Args:
        df (pd.DataFrame): DataFrame a subir
        client (Minio): Cliente de MinIO
        path (str): Ruta dentro del bucket donde se situará el objeto
        minio_object (str): Nombre del objeto destino en el bucket
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    load_dotenv()
    minio_bucket=os.getenv("MINIO_BUCKET")
    minio_groupPath=os.getenv("MINIO_GROUP_PATH")
    assert minio_bucket, "Falta MINIO_BUCKET en el entorno/.env"
    assert minio_groupPath, "Falta MINIO_GROUP_PATH en el entorno/.env"
    assert client.bucket_exists(minio_bucket), (
        f"El bucket {minio_bucket} no existe o no tienes permisos."
    )
    
    buffer.seek(0)
    length = buffer.getbuffer().nbytes

    client.put_object(
        bucket_name=minio_bucket,
        object_name=f'{minio_groupPath}/{path}/{minio_object}',
        data=buffer,
        length=length,
        content_type="application/octet-stream"
    )


cliente = crear_cliente_minio()

BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[1]

DATA_DIR = PROJECT_ROOT / "Entrega1_Pd2" / "datos" / "limpios"

WEATHER_PATH = DATA_DIR / "nyc_weather_2023_first_half.csv"

PATH_LIMPIO = "limpios"
PATH_CRUDO = "crudos"

weather = pd.read_csv(WEATHER_PATH)

subir_minio(weather, cliente, PATH_LIMPIO, "nyc_weather_2023_first_half.parquet")