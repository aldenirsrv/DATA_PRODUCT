"""
Slack notification operators for Airflow task monitoring.
Provides failure alerts and success notifications via Slack webhooks.
"""

from google.cloud import secretmanager
import requests
import os
import dotenv

dotenv.load_dotenv()


def slack_failure_alert(context):
    """
    Send Slack notification when an Airflow task fails with detailed error context.
    
    This function is designed to be used as an on_failure_callback in DAG default_args.
    It retrieves the Slack webhook URL from GCP Secret Manager and posts a formatted
    failure message with task details, execution info, and error logs.
    
    Args:
        context: Airflow context dictionary containing task instance, DAG info, and exception details
            - dag: DAG object with dag_id
            - task_instance: TaskInstance with task_id and log_url
            - execution_date: Datetime of the DAG run
            - exception: Exception object that caused the failure
    
    Returns:
        None: Prints warning if Slack webhook is not configured or if posting fails
    
    Environment Variables:
        GCP_PROJECT: Google Cloud project ID for Secret Manager access
    
    Secrets Required:
        SLACK_WEBHOOK: Slack incoming webhook URL stored in GCP Secret Manager
    
    Example:
        # In DAG configuration:
        default_args = {
            'owner': 'data@company.com',
            'on_failure_callback': slack_failure_alert,
        }
    """
    try:
        # Retrieve Slack webhook from Secret Manager
        project_id = os.getenv("GCP_PROJECT")
        if not project_id:
            print("⚠️ GCP_PROJECT environment variable not set. Cannot retrieve Slack webhook.")
            return
        
        sm = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/SLACK_WEBHOOK/versions/latest"
        
        try:
            secret_response = sm.access_secret_version(name=secret_name)
            webhook = secret_response.payload.data.decode("UTF-8")
        except Exception as e:
            print(f"⚠️ Failed to retrieve Slack webhook from Secret Manager: {e}")
            return
        
        if not webhook:
            print("⚠️ Slack webhook not configured (SLACK_WEBHOOK secret is empty).")
            return

        # Extract task context information
        dag_id = context.get("dag").dag_id if context.get("dag") else "Unknown DAG"
        task_id = context.get("task_instance").task_id if context.get("task_instance") else "Unknown Task"
        execution_date = context.get("execution_date", "Unknown Date")
        exception = context.get("exception", "No exception details available")
        
        # Get log URL if available
        log_url = "N/A"
        task_instance = context.get("task_instance")
        if task_instance and hasattr(task_instance, "log_url"):
            log_url = task_instance.log_url

        # Format Slack message with rich context
        message = {
            "text": f"🚨 *Airflow Task Failed!*",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🚨 Airflow Task Failure",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*DAG:*\n`{dag_id}`"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Task:*\n`{task_id}`"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Execution Date:*\n`{execution_date}`"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Status:*\n❌ Failed"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error:*\n```{str(exception)[:500]}```"  # Truncate long errors
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Logs:*\n<{log_url}|View Task Logs>"
                    }
                }
            ]
        }

        # Send Slack notification
        response = requests.post(webhook, json=message, timeout=10)
        response.raise_for_status()
        
        print(f"✅ Slack failure alert sent successfully for {dag_id}.{task_id}")
        
    except Exception as e:
        # Don't fail the callback if Slack posting fails
        print(f"⚠️ Failed to send Slack failure alert: {e}")


def slack_success_alert(context):
    """
    Send Slack notification when an Airflow task succeeds with execution summary.
    
    This function is designed to be used as an on_success_callback in DAG default_args
    or specific task configurations. It retrieves the Slack webhook URL from GCP Secret
    Manager and posts a formatted success message.
    
    Args:
        context: Airflow context dictionary containing task instance and DAG info
            - dag: DAG object with dag_id
            - task_instance: TaskInstance with task_id, duration, and try_number
            - execution_date: Datetime of the DAG run
    
    Returns:
        None: Prints warning if Slack webhook is not configured or if posting fails
    
    Environment Variables:
        GCP_PROJECT: Google Cloud project ID for Secret Manager access
    
    Secrets Required:
        SLACK_WEBHOOK: Slack incoming webhook URL stored in GCP Secret Manager
    
    Example:
        # For specific important tasks:
        critical_task = PythonOperator(
            task_id='critical_etl',
            python_callable=my_function,
            on_success_callback=slack_success_alert,
        )
    """
    try:
        # Retrieve Slack webhook from Secret Manager
        project_id = os.getenv("GCP_PROJECT")
        if not project_id:
            print("⚠️ GCP_PROJECT environment variable not set. Cannot retrieve Slack webhook.")
            return
        
        sm = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/SLACK_WEBHOOK/versions/latest"
        
        try:
            secret_response = sm.access_secret_version(name=secret_name)
            webhook = secret_response.payload.data.decode("UTF-8")
        except Exception as e:
            print(f"⚠️ Failed to retrieve Slack webhook from Secret Manager: {e}")
            return
        
        if not webhook:
            print("⚠️ Slack webhook not configured (SLACK_WEBHOOK secret is empty).")
            return

        # Extract task context information
        dag_id = context.get("dag").dag_id if context.get("dag") else "Unknown DAG"
        task_id = context.get("task_instance").task_id if context.get("task_instance") else "Unknown Task"
        execution_date = context.get("execution_date", "Unknown Date")
        
        # Get task duration if available
        task_instance = context.get("task_instance")
        duration = "N/A"
        if task_instance and hasattr(task_instance, "duration"):
            duration = f"{task_instance.duration:.2f}s" if task_instance.duration else "N/A"

        # Format Slack message
        message = {
            "text": f"✅ Airflow Task Succeeded: {dag_id}.{task_id}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"✅ *Task Succeeded:* `{dag_id}.{task_id}`\n*Duration:* {duration}\n*Execution Date:* `{execution_date}`"
                    }
                }
            ]
        }

        # Send Slack notification
        response = requests.post(webhook, json=message, timeout=10)
        response.raise_for_status()
        
        print(f"✅ Slack success alert sent for {dag_id}.{task_id}")
        
    except Exception as e:
        # Don't fail the callback if Slack posting fails
        print(f"⚠️ Failed to send Slack success alert: {e}")


def slack_dag_completion_alert(context):
    """
    Send Slack notification when an entire DAG completes successfully with summary statistics.
    
    This function is designed to be used as an on_success_callback at the DAG level.
    It provides a summary of the DAG run including total duration and task count.
    
    Args:
        context: Airflow context dictionary containing DAG run information
            - dag: DAG object with dag_id
            - dag_run: DagRun object with state and execution date
    
    Returns:
        None: Prints warning if Slack webhook is not configured or if posting fails
    
    Environment Variables:
        GCP_PROJECT: Google Cloud project ID for Secret Manager access
    
    Secrets Required:
        SLACK_WEBHOOK: Slack incoming webhook URL stored in GCP Secret Manager
    
    Example:
        # In DAG configuration:
        dag = DAG(
            dag_id='my_data_product',
            on_success_callback=slack_dag_completion_alert,
        )
    """
    try:
        # Retrieve Slack webhook from Secret Manager
        project_id = os.getenv("GCP_PROJECT")
        if not project_id:
            print("⚠️ GCP_PROJECT environment variable not set. Cannot retrieve Slack webhook.")
            return
        
        sm = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/SLACK_WEBHOOK/versions/latest"
        
        try:
            secret_response = sm.access_secret_version(name=secret_name)
            webhook = secret_response.payload.data.decode("UTF-8")
        except Exception as e:
            print(f"⚠️ Failed to retrieve Slack webhook from Secret Manager: {e}")
            return
        
        if not webhook:
            print("⚠️ Slack webhook not configured (SLACK_WEBHOOK secret is empty).")
            return

        # Extract DAG context information
        dag = context.get("dag")
        dag_id = dag.dag_id if dag else "Unknown DAG"
        dag_run = context.get("dag_run")
        execution_date = dag_run.execution_date if dag_run else "Unknown Date"
        
        # Count tasks
        task_count = len(dag.tasks) if dag else 0

        # Format Slack message
        message = {
            "text": f"🎉 DAG Completed: {dag_id}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🎉 DAG Completed Successfully",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*DAG:*\n`{dag_id}`"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Tasks:*\n{task_count} completed"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Execution Date:*\n`{execution_date}`"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Status:*\n✅ Success"
                        }
                    ]
                }
            ]
        }

        # Send Slack notification
        response = requests.post(webhook, json=message, timeout=10)
        response.raise_for_status()
        
        print(f"✅ Slack DAG completion alert sent for {dag_id}")
        
    except Exception as e:
        # Don't fail the callback if Slack posting fails
        print(f"⚠️ Failed to send Slack DAG completion alert: {e}")