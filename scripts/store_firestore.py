from numpy import dot
import pandas as pd, json, os
from google.cloud import firestore
import dotenv
dotenv.load_dotenv()

def store_to_firestore(**context):
    df_json = context["ti"].xcom_pull(task_ids="extract_weather_data", key="latest_df")
    df = pd.read_json(df_json)
    db = firestore.Client(project=os.getenv("GCP_PROJECT"))

    for _, row in df.iterrows():
        db.collection("weather_records").add(row.to_dict())

    print(f"Inserted {len(df)} records into Firestore.")