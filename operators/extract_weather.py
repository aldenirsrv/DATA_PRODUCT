from airflow.decorators import task
import pandas as pd, requests
from datetime import datetime
import dotenv
dotenv.load_dotenv()

# ======================================================
# 1️⃣ Extract weather - Returns data for downstream tasks
# ======================================================
@task
def extract_weather(api_url: str, latitude: float, longitude: float, metrics: str, **context):
    """
    Extract weather data from Open-Meteo API and return as JSON string.
    
    Args:
        api_url: Open-Meteo API endpoint URL
        latitude: Geographic latitude coordinate
        longitude: Geographic longitude coordinate
        metrics: Comma-separated list of weather metrics (e.g., "temperature_2m,precipitation,wind_speed_10m")
        **context: Airflow context
    
    Returns:
        str: JSON string of the extracted DataFrame containing weather data with timestamp, location, and extracted_at fields
    """
    params = {"latitude": latitude, "longitude": longitude, "hourly": metrics}
    r = requests.get(api_url, params=params)
    r.raise_for_status()
    
    data = r.json()["hourly"]
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["time"])
    df["location"] = f"{latitude},{longitude}"
    df["extracted_at"] = datetime.utcnow()
    
    print(f"✅ Extracted {len(df)} records from {api_url}.")
    
    # Return the data - Airflow automatically pushes to XCom
    return df.to_json(date_format='iso', orient='records')