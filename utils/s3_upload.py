import boto3
import os
import csv
import praw
from dotenv import load_dotenv


# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Charger les credentials AWS depuis le fichier .env
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Credentials Reddit
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')


# Fonction pour uploader un fichier sur S3
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


# Fonction pour extraire des données de Reddit
def extract_reddit_data(subreddit_name, limit=50):
    """Récupérer les posts d'un subreddit et les sauvegarder en fichier CSV."""
    
    # Initialiser l'API Reddit avec PRAW
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )

    # Sélectionner le subreddit
    subreddit = reddit.subreddit(subreddit_name)

    # Liste pour stocker les posts
    posts = []

    # Extraire les données (limite de posts définie par 'limit')
    for submission in subreddit.hot(limit=limit):
        posts.append([submission.title, submission.score, submission.url])

    # Sauvegarder les données dans un fichier CSV
    csv_file = f"{subreddit_name}_posts.csv"
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Title", "Score", "URL"])
        writer.writerows(posts)
    
    print(f"Les données Reddit ont été sauvegardées dans {csv_file}")
    
    return csv_file


if __name__ == "__main__":
    # Paramètres
    subreddit_name = 'muaythai'  # On récupère les posts du subreddit muaythai
    limit = 50  # Nombre de posts à récupérer

    # Étape 1 : Extraire les données de Reddit et les sauvegarder en CSV
    csv_file = extract_reddit_data(subreddit_name, limit)

    # Étape 2 : Uploader le fichier CSV dans le bucket S3
    upload_to_s3(csv_file, AWS_BUCKET_NAME, f"reddit/{csv_file}")
