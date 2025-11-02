FROM apache/airflow:2.10.0-python3.10

USER root
RUN apt-get update && apt-get install -y git

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy project
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/dags/scripts/

# Copy credentials securely (use volume for local testing)
# DO NOT bake credentials into the image in production.