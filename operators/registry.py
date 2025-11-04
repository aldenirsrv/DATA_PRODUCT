from operators.extract_weather import extract_weather
from operators.store_bigquery import store_to_bigquery
from operators.store_firestore import store_to_firestore
from operators.send_alerts import send_alerts
from operators.test_utils import test_send_alerts, force_error

TASK_FUNCTIONS = {
    "extract_weather": extract_weather,
    "store_to_bigquery": store_to_bigquery,
    "store_to_firestore": store_to_firestore,
    "send_alerts": send_alerts,
    "test_send_alerts": test_send_alerts,
    "force_error": force_error,
}

print(f"✅ Auto-registered {len(TASK_FUNCTIONS)} task functions from operators.")