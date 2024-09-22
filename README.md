Project Overview
This project provides a comprehensive data pipeline solution to extract, transform, and load (ETL) Reddit data into a Redshift data warehouse. The pipeline leverages a combination of tools and services including Apache Airflow, Celery, PostgreSQL, Amazon S3, AWS Glue, Amazon Athena, and Amazon Redshift.

Architecture
The pipeline is designed to:

Extract data from Reddit using its API. Store the raw data into an S3 bucket from Airflow. Transform the data using AWS Glue and Amazon Athena. Load the transformed data into Amazon Redshift for analytics and querying.


![image](https://github.com/user-attachments/assets/cfd753ef-e419-4724-a0af-82a232ccd7c2)



1. 	Reddit API: Source of the data.
2.	Apache Airflow & Celery: Orchestrates the ETL process and manages task distribution.
3.	PostgreSQL: Temporary storage and metadata management.
4.	Amazon S3: Raw data storage.
5.	AWS Glue: Data cataloging and ETL jobs.
6.	Amazon Athena: SQL-based data transformation.
7.	Amazon Redshift: Data warehousing and analytics.
