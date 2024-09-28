from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import praw
from dotenv import load_dotenv
import os
import pandas as pd  # Ajout de pandas pour gérer les fichiers CSV

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Fonction pour extraire les données de Reddit
def extract_reddit_data(**kwargs):
    # Initialiser la connexion Reddit en utilisant les variables d'environnement
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )

    # Exemple : Récupérer les 50 derniers posts du subreddit "dataengineering"
    subreddit = reddit.subreddit('calisthenics')
    posts = []
    for submission in subreddit.hot(limit=50):
        posts.append({
            'title': submission.title,
            'score': submission.score,
            'url': submission.url,
            'num_comments': submission.num_comments,  # Nombre de commentaires
            'created': datetime.fromtimestamp(submission.created),  # Date de création du post
        })

    # Log des données récupérées
    for post in posts:
        print(f"Title: {post['title']}, Score: {post['score']}, URL: {post['url']}")

    # Sauvegarder les posts dans un fichier CSV
    df = pd.DataFrame(posts)
    output_path = '/tmp/reddit_posts.csv'
    df.to_csv(output_path, index=False)
    print(f"Les données ont été sauvegardées dans {output_path}")

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
    dag=dag,
)

# Définir l'ordre des tâches (dans cet exemple, il n'y a qu'une tâche pour l'instant)
extract_task
