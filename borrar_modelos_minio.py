import os
import boto3
from dotenv import load_dotenv, find_dotenv

def borrar_carpeta_modelos_boto3_paginado():
    load_dotenv(find_dotenv())
    
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    bucket = os.getenv("MINIO_BUCKET")
    group_path = os.getenv("MINIO_GROUP_PATH")

    print("Conectando a MinIO a través de boto3...")
    
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    prefijo_borrar = f"{group_path}/modelos"
    archivos_borrados = 0
    continuar_borrando = True
    token_paginacion = None # Para saber por qué página vamos

    print(f"Buscando y destruyendo todo en: {prefijo_borrar}")

    try:
        while continuar_borrando:
            # Pedimos la lista de archivos (con el token si estamos en la página 2, 3...)
            if token_paginacion:
                response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefijo_borrar, ContinuationToken=token_paginacion)
            else:
                response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefijo_borrar)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    archivo_key = obj['Key']
                    # Borramos el archivo
                    s3_client.delete_object(Bucket=bucket, Key=archivo_key)
                    archivos_borrados += 1
                    
                    # Imprimimos 1 de cada 50 para no saturar la consola si hay miles
                    if archivos_borrados % 50 == 0:
                        print(f" - Llevamos {archivos_borrados} archivos borrados...")
                
                # ¿Hay más páginas?
                if response.get('IsTruncated'): # IsTruncated = True significa que hay más de 1000
                    token_paginacion = response.get('NextContinuationToken')
                else:
                    continuar_borrando = False # Ya no hay más
            else:
                continuar_borrando = False # La carpeta estaba vacía

        if archivos_borrados > 0:
            print(f"\n¡Exterminio completado! Se han borrado un total de {archivos_borrados} archivos.")
            print("MinIO está limpio. Puedes comprobarlo en la interfaz web (quizás tengas que refrescar la página F5).")
        else:
            print(f"\nLa carpeta '{prefijo_borrar}' no existía o ya estaba totalmente vacía.")
            
    except Exception as e:
        print(f"Error crítico en MinIO: {e}")

if __name__ == "__main__":
    borrar_carpeta_modelos_boto3_paginado()