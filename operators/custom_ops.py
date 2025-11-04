from airflow.decorators import task
import pandas as pd, os, requests
from datetime import datetime
from google.cloud import bigquery, firestore, secretmanager
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import dotenv
dotenv.load_dotenv()

# ======================================================
# 1️⃣ Extract weather (unchanged)
# ======================================================
@task
def extract_weather(api_url: str, latitude: float, longitude: float, metrics: str, **context):
    params = {"latitude": latitude, "longitude": longitude, "hourly": metrics}
    r = requests.get(api_url, params=params)
    r.raise_for_status()
    data = r.json()["hourly"]
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["time"])
    df["location"] = f"{latitude},{longitude}"
    df["extracted_at"] = datetime.utcnow()
    context["ti"].xcom_push(key="latest_df", value=df.to_json())
    print(f"✅ Extracted {len(df)} records from {api_url}.")


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


# ======================================================
# 4️⃣ Send alerts
# ======================================================
@task
def send_alerts(recipient: str, **context):
    df_json = context["ti"].xcom_pull(task_ids="extract_weather_data", key="latest_df")
    df = pd.read_json(df_json)
    latest = df.iloc[-1]
    alerts = []
    if latest["temperature_2m"] < 0:
        alerts.append("❄️ Freezing temperature")
    if latest["wind_speed_10m"] > 60:
        alerts.append("🌪️ Strong wind")
    if latest["precipitation"] > 10:
        alerts.append("🌧️ Heavy rain")

    if not alerts:
        print("No alerts triggered.")
        return "No alerts"

    sm = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{os.getenv('GCP_PROJECT')}/secrets/SENDGRID_API_KEY/versions/latest"
    api_key = sm.access_secret_version(name=secret_name).payload.data.decode("UTF-8")

    message = Mail(
        from_email="weatherbot@example.com",
        to_emails=recipient,
        subject="⚠️ Weather Alert",
        plain_text_content="\n".join(alerts),
    )
    SendGridAPIClient(api_key).send(message)
    print(f"✅ Sent {len(alerts)} alerts to {recipient}.")
    return f"Sent {len(alerts)} alerts"

@task
def test_send_alerts(recipient: str, **context):
    print(f"✅ Sent alerts to {recipient}.")
    return f"Sent to {recipient}"

# Registry
TASK_FUNCTIONS = {
    "extract_weather": extract_weather,
    "store_to_bigquery": store_to_bigquery,
    "store_to_firestore": store_to_firestore,
    "send_alerts": send_alerts,
    "test_send_alerts": test_send_alerts,
}