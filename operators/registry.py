
"""
Central registry for all task functions.
Import all your task functions here and register them in TASK_FUNCTIONS dict.
"""
from operators.extract_weather import extract_weather
from operators.store_bigquery import store_to_bigquery
from operators.store_firestore import store_to_firestore
from operators.send_alerts import send_alerts
from operators.test_utils import test_send_alerts, force_error

# Add more imports as you create new operators
# from operators.sales_operators import extract_sales, transform_sales
# from operators.inventory_operators import sync_inventory

# ======================================================
# Global Task Registry
# ======================================================
TASK_FUNCTIONS = {
    "extract_weather": extract_weather,
    "store_to_bigquery": store_to_bigquery,
    "store_to_firestore": store_to_firestore,
    "send_alerts": send_alerts,
    "test_send_alerts": test_send_alerts,
    "force_error": force_error,
     # Add more functions here as you build them
}

print(f"✅ Auto-registered {len(TASK_FUNCTIONS)} task functions from operators.")
