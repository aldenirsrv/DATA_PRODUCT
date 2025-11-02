import pandas as pd, json, os
from google.cloud import firestore, bigquery
import dotenv
dotenv.load_dotenv()

def store_to_bigquery(**context):
    df_json = context["ti"].xcom_pull(task_ids="extract_weather_data", key="latest_df")
    df = pd.read_json(df_json)
    project_id = os.getenv("GCP_PROJECT")
    dataset_id = "weather_data"
    table_id = "weather_records"

    bq_client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["ingestion_time"] = pd.Timestamp.now(tz='UTC')

    job = bq_client.load_table_from_dataframe(
        df, table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"✅ Inserted {len(df)} records into BigQuery table {table_ref}.")