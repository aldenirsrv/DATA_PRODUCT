from airflow.decorators import task

@task
def test_send_alerts(recipient: str, **context):
    print(f"✅ Sent test alert to {recipient}.")
    return f"Sent to {recipient}"

@task
def force_error(**context):
    raise ValueError("💥 Simulated failure for Slack test")