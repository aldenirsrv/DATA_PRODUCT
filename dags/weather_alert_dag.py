from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract_weather import extract_weather
from scripts.store_firestore import store_to_firestore
from scripts.store_bigquery import store_to_bigquery
from scripts.send_email import send_alerts

default_args = {"owner": "local-user", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="weather_alerts_firebase_local",
    start_date=datetime(2025, 11, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    default_args=default_args,
) as dag:
    extract = PythonOperator(task_id="extract_weather_data", python_callable=extract_weather)
    store = PythonOperator(task_id="store_in_firestore", python_callable=store_to_firestore)
    store_bq = PythonOperator(task_id="store_in_bigquery",python_callable=store_to_bigquery)
    alert = PythonOperator(task_id="send_alerts_if_needed", python_callable=send_alerts)
    extract >> store >> store_bq >> alert