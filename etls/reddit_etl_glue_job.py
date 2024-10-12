import sys
import boto3
import time
from pyspark.sql import functions as F  # Importer les fonctions SQL correctes
from pyspark.sql.functions import col, min, max
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Récupérer les paramètres du Job (on utilise JOB_NAME standard)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialiser le contexte Glue et Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Variables des buckets S3
raw_bucket = "s3://reddit-data-pipeline-raw/raw_data/muaythai_posts.csv"
# Spécifie le chemin vers le dossier "transformed-data" dans le bucket de sortie
transformed_bucket = "s3://reddit-data-pipeline-transformed/transformed-data/"

# Lecture du fichier CSV brut depuis S3
raw_data = spark.read.option("header", "true").csv(raw_bucket)

# --- Transformation 1: Nettoyage des données ---
# Supprimer les lignes où 'title' ou 'url' sont nulles
clean_data = raw_data.dropna(subset=["title", "url"])

# --- Transformation 2: Normalisation du score ---
# Convertir la colonne 'score' en entier et normaliser entre 0 et 1
score_min = clean_data.select(min(col("score").cast("int"))).collect()[0][0]
score_max = clean_data.select(max(col("score").cast("int"))).collect()[0][0]

# Ajouter une nouvelle colonne 'score_normalized'
clean_data = clean_data.withColumn(
    "score_normalized",
    (col("score").cast("int") - score_min) / (score_max - score_min)
)

# --- Transformation 3: Conversion en minuscules ---
# Utiliser F.lower pour mettre tous les titres en minuscules
clean_data = clean_data.withColumn("title", F.lower(col("title")))

# --- Écriture des données transformées ---
# Écrire dans S3 sous forme de fichier Parquet, en un seul fichier avec coalesce(1)
clean_data.coalesce(1).write.mode("overwrite").parquet(transformed_bucket)

# --- Renommer le fichier généré ---
# Utiliser Boto3 pour renommer le fichier Parquet généré
s3 = boto3.client('s3')
bucket_name = "reddit-data-pipeline-transformed"
prefix = "transformed-data/"

# Lister les fichiers dans le dossier transformed-data
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Chercher le fichier Parquet généré
for obj in response.get('Contents', []):
    if obj['Key'].endswith('.parquet'):
        # Nouveau nom de fichier souhaité
        new_key = f"{prefix}muaythai_posts_transformed.parquet"
        
        # Renommer le fichier (copier puis supprimer l'original)
        s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': obj['Key']}, Key=new_key)
        s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
        print(f"Fichier renommé en {new_key}")

# Automatisation du Crawler AWS Glue
crawler_name = "muay_thai_reddit_crawler"  # On utilise le crawler déjà créé

def start_crawler(crawler_name):
    glue_client = boto3.client('glue')  # Initialiser le client Glue

    try:
        # Démarrer le Crawler
        glue_client.start_crawler(Name=crawler_name)
        print(f"Crawler {crawler_name} démarré.")

        # Attendre la fin du Crawler (optionnel)
        while True:
            response = glue_client.get_crawler(Name=crawler_name)
            state = response['Crawler']['State']
            if state == 'READY':
                print(f"Crawler {crawler_name} terminé.")
                break
            else:
                print(f"Crawler {crawler_name} en cours...")
                time.sleep(30)  # Attendre 30 secondes avant de vérifier à nouveau l'état du Crawler
    except Exception as e:
        print(f"Erreur lors du démarrage du Crawler: {str(e)}")

# Démarrer le Crawler pour cataloguer les données transformées
start_crawler(crawler_name)

# Terminer le Job Glue
job.commit()
