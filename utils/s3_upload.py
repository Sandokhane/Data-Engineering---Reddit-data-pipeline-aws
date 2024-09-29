import boto3
import os
from dotenv import load_dotenv




# Charger les variables d'envir

load_dotenv()


# Charger les credentiels AWS depuis le fichier .env contenant les vars d'envir
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')



def upload_to_s3(file_name, bucket, object_name=None):
    """Upload un fichier sur S3.

    :param file_name: Chemin du fichier local à uploader
    :param bucket: Nom du bucket S3
    :param object_name: Nom de l'objet S3. Si non défini, utilise le nom du fichier local
    """

    # Si aucun nom d'objet n'est fourni, utiliser le nom du fichier
    if object_name is None:
        object_name = file_name

    # Initialiser la connexion S3
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)


    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Fichier {file_name} uploadé avec succès dans {bucket}/{object_name}")
    except Exception as e:
        print(f"Erreur lors de l'upload du fichier {file_name} : {e}")