import os
import boto3
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

# Borramos el archivo fantasma que dibuja la carpeta
clave_fantasma = f"{os.getenv('MINIO_GROUP_PATH')}/modelos/"
s3.delete_object(Bucket=os.getenv("MINIO_BUCKET"), Key=clave_fantasma)

print("Holograma destruido. Refresca la web de MinIO (F5).")