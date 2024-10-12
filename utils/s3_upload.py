import boto3
import os
import csv
import praw
from dotenv import load_dotenv
import logging

# Initialiser les Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Charger les credentials AWS depuis le fichier .
# AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
# AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_NAME = os.getenv('S3_BUCKET_RAW')



# Fonction pour uploader un fichier sur S3
def upload_to_s3(file_name, bucket, object_name=None):
    """Upload un fichier sur S3.

    :param file_name: Chemin du fichier local à uploader
    :param bucket: Nom du bucket S3
    :param object_name: Nom de l'objet S3. Si non défini, utilise le nom du fichier local
    """
    # Si aucun nom d'objet n'est fourni, utiliser le nom du fichier
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Initialiser la connexion S3
    s3_client = boto3.client('s3')

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Fichier {file_name} uploadé avec succès dans {bucket}/{object_name}")

    except boto3.exceptions.S3UploadFailedError as e:
        logger.error(f"Ereur lors de l'upload vers S3: {e}")
    except Exception as e:
        logger.error(f"Erreur inattendue : {e}")




if __name__ == "__main__":
    csv_file = os.path.join(os.getcwd(), 'data/muay_thai_reddit_posts_raw.csv')

    # Nom du Bucket
    bucket_name = AWS_BUCKET_NAME


    # Construire le chemin dans S3 sous un dossier "raw"
    # Construire le chemin dans S3 sous le dossier "raw_data"
    s3_path = f"raw_data/{os.path.basename(csv_file)}"

    # Uploader le fichier dans S3 sous le chemin raw_data/
    upload_to_s3(csv_file, bucket_name, s3_path)


   

