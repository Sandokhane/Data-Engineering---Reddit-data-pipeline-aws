import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils.s3_upload import upload_to_s3
import praw
from dotenv import load_dotenv
import pandas as pd
from utils.s3_upload import upload_to_s3  # Import de votre module s3_upload.py


# Charger les variables d'environnement
load_dotenv()

# Configurer l'API Reddit avec PRAW
def extract_reddit_data(**kwargs):
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )


    # Boucle pour parcourir les 50 posts les plus "hot"
    subreddit = reddit.subreddit('muaythai')
    posts = []

    for submission in subreddit.hot(limit=50):
        posts.append({
            'title': submission.title,
            'score': submission.score,
            'url': submission.url,
            'author': submission.author.name if submission.author else "Deleted", # Auteur du post
            'num_comments': submission.num_comments,  # Nombre de commentaires
            'created_utc': datetime.fromtimestamp(submission.created_utc),  # Date de création du post
            'link_flair_text': submission.link_flair_text, # Flair du post si disponible
            'upvote_ratio': submission.upvote_ratio, # Ratio d'upvotes
            'title_length': len(submission.title), # Longeur du titre
            'is_self': submission.is_self, # Si c'est un post text (True) ou nope
            'total_awards_received': submission.total_awards_received, # Nombre de récompenses du post
            'over_18': submission.over_18, # NSFW (True si le post est marqué comme explicite)
            'distinguised': submission.distinguished, # Épinglé par un modérateur ou non
            'subreddit_subscribers': submission.subreddit_subscribers # Nombre d'abonnées au subreddit
        })

    # Convertir les données en DataFrame
    posts_df = pd.DataFrame(posts)

    
    # Sauvegarder les posts dans un csv
    file_path = os.path.join(os.getcwd(), 'data/muay_thai_reddit_posts_raw.csv')
    posts_df.to_csv(file_path, index=False)


    # Uploader sur S3
    upload_to_s3(file_path, os.getenv('S3_BUCKET_RAW'), 'raw_data/muay_thai_reddit_posts_raw.csv')



# Définition des arguments par défaut du DAG
default_args = {
    'owner': 'Data_Mediator',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
dag = DAG(
    'reddit_data_pipeline_muay_thai_posts',
    default_args=default_args,
    description='Un DAG pour extraire des données des posts de Muay Thaï dans Reddit',
    schedule_interval=timedelta(days=1),  # Exécution quotidienne
)

# Tâche Python pour extraire les données de Reddit
extract_task = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=extract_reddit_data,
    dag=dag,
)


    extract_task
