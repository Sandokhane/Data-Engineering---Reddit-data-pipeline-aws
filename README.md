# Data-Engineering---Reddit-data-pipeline-aws

**Project Overview**

This project provides a comprehensive data pipeline solution to extract, transform, and load (ETL) Reddit data into a Redshift data warehouse. The pipeline leverages a combination of tools and services including Apache Airflow, Celery, PostgreSQL, Amazon S3, AWS Glue, Amazon Athena, and Amazon Redshift.

Architecture
The pipeline is designed to:

Extract data from Reddit using its API. Store the raw data into an S3 bucket from Airflow. Transform the data using AWS Glue and Amazon Athena. Load the transformed data into Amazon Redshift for analytics and querying.

![img Architecture 2](https://github.com/user-attachments/assets/05769379-9ab6-43a1-8a13-bf286ddfbe70)


For the architecture diagram we've used the following website to build it [Excalidraw](https://excalidraw.com/)

1. Reddit API: Source of the data.
2.	Apache Airflow: Orchestrates the ETL process and manages task distribution.
3.	PostgreSQL: Temporary storage and metadata management.
4.	Amazon S3: Raw and transformed data storage.
5.	AWS Glue: Data cataloging and ETL jobs (transforming the Data using PySpark).
6.	Amazon Athena: SQL-based data transformation and querying directly on S3.
7.	Amazon Redshift: Data warehousing and analytics.
8.	Microsoft Power BI: For Data Visualisation and Business Intelligence.


### Remarque

### Configuration des variables d'environnement

Avant d'exécuter ce projet, vous devez créer un fichier `.env` à la racine du projet, en vous basant sur le fichier `.env.example` fourni.

Voici les étapes à suivre :

1. Copiez le fichier `.env.example` :
   ```bash
   cp .env.example .env
2. Remplissez le fichier .env avec vos propres identifiants API Reddit. Vous pouvez obtenir ces identifiants en créant une application Reddit sur Reddit Apps.
