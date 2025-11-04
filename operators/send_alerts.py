from airflow.decorators import task
from google.cloud import secretmanager
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import pandas as pd, os

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