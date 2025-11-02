import requests, pandas as pd
from datetime import datetime

def extract_weather(**context):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 49.2827,
        "longitude": -123.1207,
        "hourly": "temperature_2m,precipitation,wind_speed_10m"
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()["hourly"]
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["time"])
    df["location"] = "Vancouver"
    df["extracted_at"] = datetime.utcnow()
    context["ti"].xcom_push(key="latest_df", value=df.to_json())