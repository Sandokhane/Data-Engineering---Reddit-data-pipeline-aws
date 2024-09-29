from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils.s3_upload import upload_to_s3
import praw
import os
import pandas as pd
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configurer l'API Reddit avec PRAW
def extract_reddit_data(**kwargs):
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )
    
    subreddit = reddit.subreddit('muaythai')
    posts = []
    
    for submission in subreddit.hot(limit=50):
        posts.append([submission.title, submission.score, submission.url])
    
    # Convertir les posts en DataFrame
    posts_df = pd.DataFrame(posts, columns=['Title', 'Score', 'URL'])
    
    # Définir le chemin vers le dossier `data`
    file_path = os.path.join(os.getcwd(), 'data/muay_thai_reddit_posts.csv')
    
    # Sauvegarder les données dans un fichier CSV dans le dossier `data`
    posts_df.to_csv(file_path, index=False)
    
    # Upload le fichier CSV sur S3
    upload_to_s3(file_path, os.getenv('S3_BUCKET_NAME'), 'muay_thai_reddit_posts.csv')
    print(f"Données sauvegardées dans {file_path} et uploadées sur S3 avec succès")

# Configurer le DAG
default_args = {
    'owner': 'Sandokhane',
    'start_date': days_ago(1),
}

with DAG(
    'reddit_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # Tâche d'extraction des données Reddit
    extract_task = PythonOperator(
        task_id='extract_reddit_data',
        python_callable=extract_reddit_data,
        provide_context=True
    )

    extract_task
