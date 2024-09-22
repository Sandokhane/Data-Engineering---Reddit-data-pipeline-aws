from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import praw
from dotenv import load_dotenv
import os

# Fonction pour extraire les données de Reddit
def extract_reddit_data(**kwargs):
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )

    # Exemple : Récupérer les 10 derniers posts du subreddit "dataengineering"
    subreddit = reddit.subreddit('dataengineering')
    posts = []
    for submission in subreddit.hot(limit=10):
        posts.append({
            'title': submission.title,
            'score': submission.score,
            'url': submission.url,
        })

    # Log des données récupérées
    for post in posts:
        print(f"Title: {post['title']}, Score: {post['score']}, URL: {post['url']}")

# Définition des arguments par défaut du DAG
default_args = {
    'owner': 'Sandokhane',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
dag = DAG(
    'reddit_data_pipeline',
    default_args=default_args,
    description='Un DAG pour extraire des données de Reddit',
    schedule_interval=timedelta(days=1),  # Exécution quotidienne
)

# Tâche Python pour extraire les données de Reddit
extract_task = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=extract_reddit_data,
    provide_context=True,  # Permet de passer les **kwargs pour extraire des informations supplémentaires si nécessaire
    dag=dag,
)

# Définir l'ordre des tâches (dans cet exemple, il n'y a qu'une tâche pour l'instant)
extract_task
