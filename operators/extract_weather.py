from airflow.decorators import task
import pandas as pd, requests
from datetime import datetime
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
