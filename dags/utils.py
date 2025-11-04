import yaml, json, jsonschema, os
from typing import Any
import os
import requests
from google.cloud import secretmanager

def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def validate_yaml(yaml_path: str, schema_path: str) -> dict:
    """Validate YAML config against schema. Raises ValidationError if invalid."""
    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f)
    with open(schema_path, "r") as f:
        schema = json.load(f)
    jsonschema.validate(instance=data, schema=schema)
    return data

def safe_get(d: dict, key: str, default: Any = None):
    return d[key] if key in d else default


def slack_failure_alert(context):
    """
    Universal Slack alert for Airflow task failures.
    Works for all dynamically generated DAGs.
    """
    sm = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{os.getenv('GCP_PROJECT')}/secrets/SLACK_WEBHOOK/versions/latest"
    webhook = sm.access_secret_version(name=secret_name).payload.data.decode("UTF-8")
    if not webhook:
        print("⚠️ Slack webhook not configured (SLACK_WEBHOOK).")
        return

    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    exception = context.get("exception")

    message = (
        f"🚨 *Airflow Task Failed!*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Execution Date:* `{execution_date}`\n"
        f"*Error:* `{exception}`\n"
        f"*Logs:* {log_url}"
    )

    try:
        requests.post(webhook, json={"text": message})
    except Exception as e:
        print(f"⚠️ Failed to send Slack message: {e}")