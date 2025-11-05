from airflow.decorators import task
from google.cloud import secretmanager
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import pandas as pd, os

# ======================================================
# 4️⃣ Send alerts
# ======================================================
@task
def send_alerts(
    recipient: str,
    upstream_data: str = None,
    temperature_threshold: float = 0,
    wind_speed_threshold: float = 60,
    precipitation_threshold: float = 10,
    **context
):
    """
    Analyze weather data and send email alerts via SendGrid if configurable thresholds are exceeded.
    
    Args:
        recipient: Email recipient address
        upstream_data: JSON string from upstream task containing weather data (automatically injected)
        temperature_threshold: Alert if temperature falls below this value in °C (default: 0)
        wind_speed_threshold: Alert if wind speed exceeds this value in km/h (default: 60)
        precipitation_threshold: Alert if precipitation exceeds this value in mm (default: 10)
        **context: Airflow context
    
    Returns:
        dict: Status metadata containing alert status, number of alerts sent, recipient, and SendGrid response code
    """
    if not upstream_data:
        # ✅ Graceful handling when no data available
        print("⚠️ No upstream data provided. Sending generic success notification.")
        subject = "✅ Weather Data Pipeline Success"
        body = "Weather data pipeline completed successfully."
        alerts = []
    else:
        # ✅ Parse data from any upstream task
        df = pd.read_json(upstream_data, orient='records')
        
        if df.empty:
            print("⚠️ No weather data to analyze.")
            return {"status": "no_data", "alerts_sent": 0}
        
        latest = df.iloc[-1]
        
        # ✅ Configurable thresholds via YAML
        alerts = []
        if "temperature_2m" in latest and latest["temperature_2m"] < temperature_threshold:
            alerts.append(f"❄️ Freezing temperature: {latest['temperature_2m']}°C")
        
        if "wind_speed_10m" in latest and latest["wind_speed_10m"] > wind_speed_threshold:
            alerts.append(f"🌪️ Strong wind: {latest['wind_speed_10m']} km/h")
        
        if "precipitation" in latest and latest["precipitation"] > precipitation_threshold:
            alerts.append(f"🌧️ Heavy rain: {latest['precipitation']} mm")
        
        if not alerts:
            # ✅ Return detailed status even when no alerts
            return {
                "status": "no_alerts",
                "alerts_sent": 0,
                "latest_temp": latest.get("temperature_2m"),
                "latest_wind": latest.get("wind_speed_10m"),
                "latest_precip": latest.get("precipitation")
            }
        
        # ✅ Rich, formatted email with context
        subject = "⚠️ Weather Alert"
        body = f"""
                Weather Alert Triggered!

                Location: {latest.get('location', 'N/A')}
                Timestamp: {latest.get('timestamp', 'N/A')}

                Alerts:
                {chr(10).join(f"• {alert}" for alert in alerts)}

                Current Conditions:
                - Temperature: {latest.get('temperature_2m', 'N/A')}°C
                - Wind Speed: {latest.get('wind_speed_10m', 'N/A')} km/h
                - Precipitation: {latest.get('precipitation', 'N/A')} mm

                ---
                Automated alert from Weather Data Pipeline
                        """.strip()
    
    # ✅ Comprehensive error handling
    try:
        project_id = os.getenv("GCP_PROJECT")
        if not project_id:
            raise ValueError("GCP_PROJECT environment variable not set")
        
        sm = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/SENDGRID_API_KEY/versions/latest"
        api_key = sm.access_secret_version(name=secret_name).payload.data.decode("UTF-8")

        message = Mail(
            from_email= os.getenv("SEND_GRID_EMAIL"),
            to_emails=recipient,
            subject=subject,
            plain_text_content=body,
        )
        
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        
        print(f"✅ Sent {len(alerts)} alert(s) to {recipient}.")
        
        # ✅ Return detailed metadata
        return {
            "status": "success",
            "alerts_sent": len(alerts),
            "recipient": recipient,
            "sendgrid_status": response.status_code
        }
    
    except Exception as e:
        print(f"❌ Failed to send alert: {str(e)}")
        # ✅ Don't fail pipeline if email fails
        return {
            "status": "email_failed",
            "error": str(e),
            "alerts_triggered": len(alerts) if alerts else 0
        }