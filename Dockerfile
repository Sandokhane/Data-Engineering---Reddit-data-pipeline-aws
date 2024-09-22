FROM apache/airflow:2.3.0

# Copie le fichier requirements.txt dans le conteneur
COPY requirements.txt /requirements.txt

# Installe les dépendances listées dans requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
