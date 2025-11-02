# using SendGrid's Python Library:  https://github.com/sendgrid/sendgrid-python
import json, pandas as pd, os
from google.cloud import secretmanager
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import dotenv
dotenv.load_dotenv()

def send_alerts(**context):
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
        return "No alerts"

    try:
        sm = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{os.getenv('GCP_PROJECT')}/secrets/SENDGRID_API_KEY/versions/latest"
        api_key = sm.access_secret_version(name=secret_name).payload.data.decode("UTF-8")

        message = Mail(
            from_email="weatherbot@example.com",
            to_emails="recipient@example.com",
            subject="⚠️ Weather Alert",
            plain_text_content="\n".join(alerts),
        )
        SendGridAPIClient(api_key).send(message)
        return f"Sent {len(alerts)} alerts"
    except Exception as e:
        print(e.message)
        return f"Failed to send alerts: {e.message}"
