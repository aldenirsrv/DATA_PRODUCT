from airflow.decorators import task
from google.cloud import bigquery
import pandas as pd, os
import dotenv
dotenv.load_dotenv()

# ======================================================
# 2️⃣ Store to BigQuery - Accepts upstream data
# ======================================================
@task
def store_to_bigquery(dataset: str, table: str, upstream_data: str = None, **context):
    """
    Store data to BigQuery with automatic schema detection and timestamp enrichment.
    
    Args:
        dataset: BigQuery dataset name
        table: BigQuery table name
        upstream_data: JSON string from upstream task (automatically injected)
        **context: Airflow context
    
    Returns:
        dict: Status metadata containing success status, rows_inserted count, and table reference
    """
    if not upstream_data:
        raise ValueError("No upstream data provided. Ensure 'depends_on_output' is set in YAML.")
    
    # Parse the data from upstream task
    df = pd.read_json(upstream_data, orient='records')
    
    project_id = os.getenv("GCP_PROJECT")
    if not project_id:
        raise ValueError("GCP_PROJECT environment variable not set")
    
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
    
    # Return confirmation for potential downstream tasks
    return {"status": "success", "rows_inserted": len(df), "table": table_ref}

