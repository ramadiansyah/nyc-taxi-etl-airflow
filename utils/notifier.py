"""
notifier.py

Utility functions to send notifications from Airflow DAGs to Discord via webhook.

Functions:
    - failure_callback: Called when a task fails; sends an alert message to Discord.
    - success_callback: Called when a task succeeds; sends a success message to Discord.
    - send_discord_alert: Sends a message to a specified Discord webhook URL.
"""

# utils/notifier.py

import requests

def create_failure_callback(webhook_url: str):
    """
    Factory that returns a failure callback function with a bound webhook URL.
    """
    def failure_callback(context):
        dag_id = context.get("dag").dag_id
        run_id = context.get("run_id")
        task_id = context.get("task_instance").task_id
        execution_date = context.get("execution_date")

        message = (
            f"🚨 **Airflow Alert**\n"
            f"DAG `{dag_id}` → Task `{task_id}` FAILED\n"
            f"🗓️ `{execution_date}`\n"
            f"🧩 Run ID: `{run_id}`"
        )
        send_discord_alert(message, webhook_url)
    return failure_callback


def create_success_callback(webhook_url: str):
    """
    Factory that returns a success callback function with a bound webhook URL.
    """
    def success_callback(context):
        dag_id = context.get("dag").dag_id
        run_id = context.get("run_id")
        task_id = context.get("task_instance").task_id
        execution_date = context.get("execution_date")

        message = (
            f"✅ **Airflow Success**\n"
            f"DAG `{dag_id}` → Task `{task_id}` SUCCEEDED\n"
            f"🗓️ `{execution_date}`\n"
            f"🧩 Run ID: `{run_id}`"
        )
        send_discord_alert(message, webhook_url)
    return success_callback


def send_discord_alert(message: str, webhook_url: str):
    payload = {"content": message}
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to send Discord alert: {e}")
