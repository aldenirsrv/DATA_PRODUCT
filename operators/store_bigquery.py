from airflow.decorators import task
from google.cloud import bigquery
import pandas as pd, os
import dotenv
dotenv.load_dotenv()

# ======================================================
# 2️⃣ Store to BigQuery (kept as-is)
# ======================================================
@task
def store_to_bigquery(dataset: str, table: str, **context):
    df_json = context["ti"].xcom_pull(task_ids="extract_weather_data", key="latest_df")
    df = pd.read_json(df_json)
    project_id = os.getenv("GCP_PROJECT")
    table_ref = f"{project_id}.{dataset}.{table}"

    client = bigquery.Client(project=project_id)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["ingestion_time"] = pd.Timestamp.now(tz="UTC")

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    print(f"✅ Inserted {len(df)} rows into {table_ref}.")

