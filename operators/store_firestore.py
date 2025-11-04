from airflow.decorators import task
from google.cloud import firestore
import pandas as pd, os

# ======================================================
# 3️⃣ Store to Firestore
# ======================================================
@task
def store_to_firestore(collection: str, **context):
    df_json = context["ti"].xcom_pull(task_ids="extract_weather_data", key="latest_df")
    df = pd.read_json(df_json)
    db = firestore.Client(project=os.getenv("GCP_PROJECT"))
    for _, row in df.iterrows():
        db.collection(collection).add(row.to_dict())
    print(f"✅ Stored {len(df)} records into Firestore collection '{collection}'.")
