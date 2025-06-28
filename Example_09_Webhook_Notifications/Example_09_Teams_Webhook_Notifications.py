"""
Airflow DAG demonstrating Microsoft Teams webhook notifications
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
try:
    from airflow.sdk import Variable
except ImportError:
    from airflow.models import Variable  
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow-admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'EXAMPLE_09_teams_notifications',
    default_args=default_args,
    description='Example DAG 09 - Microsoft Teams webhook notifications',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['example', 'notifications', 'teams', 'webhooks'],
)

def send_teams_notification(context, message_type="info"):
    """
    Function to send Teams notification to EPAM webhook
    """
    import requests
    from datetime import datetime
    
    try:
        # Get webhook URL from Airflow Variable (recommended for security)
        try:
            teams_webhook_url = Variable.get("teams_webhook_url")
        except Exception as var_error:
            print(f"❌ Failed to get teams_webhook_url variable: {str(var_error)}")
            return
        
        if not teams_webhook_url:
            print("❌ Teams webhook URL not found in Airflow Variables")
            return
        
        # Get task instance info
        task_instance = context['task_instance']
        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id
        # Handle both old and new Airflow versions
        execution_date = context.get('logical_date') or context.get('execution_date')
        run_id = context.get('run_id', 'N/A')
        
        # Get task duration if available
        start_date = task_instance.start_date
        end_date = task_instance.end_date
        duration = "N/A"
        if start_date and end_date:
            duration = str(end_date - start_date)
        
        # Color coding and emojis based on message type
        theme_colors = {
            "success": "00FF00",  # Green
            "failure": "FF0000",  # Red
            "info": "0078D4",     # Blue
            "warning": "FFA500",  # Orange
            "start": "0078D4"     # Blue
        }
        
        # Create message content based on type
        if message_type == "success":
            title = f"✅ Task Completed Successfully"
            subtitle = f"DAG: {dag_id} | Task: {task_id}"
            summary = f"Task {task_id} in DAG {dag_id} completed successfully"
        elif message_type == "failure":
            title = f"❌ Task Failed"
            subtitle = f"DAG: {dag_id} | Task: {task_id}"
            summary = f"Task {task_id} in DAG {dag_id} failed"
        elif message_type == "start":
            title = f"🚀 DAG Started"
            subtitle = f"DAG: {dag_id}"
            summary = f"DAG {dag_id} execution started"
        else:
            title = f"ℹ️ DAG Notification"
            subtitle = f"DAG: {dag_id} | Task: {task_id}"
            summary = f"Notification from DAG {dag_id}"
        
        # Create facts array
        facts = [
            {"name": "DAG ID", "value": dag_id},
            {"name": "Task ID", "value": task_id},
            {"name": "Run ID", "value": run_id},
            {"name": "Execution Date", "value": execution_date.strftime("%Y-%m-%d %H:%M:%S UTC") if execution_date else "N/A"},
            {"name": "Status", "value": message_type.upper()},
        ]
        
        # Add duration for completed tasks
        if duration != "N/A":
            facts.append({"name": "Duration", "value": duration})
        
        # Teams message format for Office 365 connector
        message = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": summary,
            "themeColor": theme_colors.get(message_type, "0078D4"),
            "sections": [
                {
                    "activityTitle": title,
                    "activitySubtitle": subtitle,
                    "facts": facts,
                    "markdown": True
                }
            ]
        }
        
        # Add potential actions for failed tasks
        if message_type == "failure":
            message["potentialAction"] = [
                {
                    "@type": "OpenUri",
                    "name": "View in Airflow",
                    "targets": [
                        {
                            "os": "default",
                            "uri": f"http://localhost:8080/dags/{dag_id}/grid"  # Update with your Airflow URL
                        }
                    ]
                }
            ]
        
        # Send the message
        response = requests.post(
            teams_webhook_url,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(message),
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"Failed to send Teams notification: {response.status_code} - {response.text}")
            # Don't raise exception to avoid breaking the DAG
        else:
            print(f"✅ Teams notification sent successfully for {message_type}")
            
    except Exception as e:
        print(f"❌ Error sending Teams notification: {str(e)}")
        # Don't raise exception to avoid breaking the DAG

# Success callback function
def success_callback(context):
    send_teams_notification(context, "success")

# Failure callback function  
def failure_callback(context):
    send_teams_notification(context, "failure")

# Example tasks
task_01_start_notification = PythonOperator(
    task_id='task_01_start_notification',
    python_callable=lambda **context: send_teams_notification(context, "start"),
    dag=dag,
)

# Example data processing task
task_02_process_data = BashOperator(
    task_id='task_02_process_data',
    bash_command='echo "Processing data..." && sleep 10 && echo "Data processing complete"',
    on_success_callback=success_callback,
    on_failure_callback=failure_callback,
    dag=dag,
)

# Example Python task
def example_python_task(**context):
    """Example Python function"""
    print("Running Python task...")
    import time
    time.sleep(5)
    print("Python task completed")
    return "Task completed successfully"

task_03_python_processing = PythonOperator(
    task_id='task_03_python_processing',
    python_callable=example_python_task,
    on_success_callback=success_callback,
    on_failure_callback=failure_callback,
    dag=dag,
)

# DAG completion notification
def dag_completion_notification(**context):
    """Send notification when entire DAG completes"""
    send_teams_notification(context, "success")
    
    # Additional summary notification
    dag_id = context['dag'].dag_id
    execution_date = context.get('logical_date') or context.get('execution_date')
    
    summary_message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": f"DAG {dag_id} execution completed",
        "themeColor": "00FF00",
        "sections": [
            {
                "activityTitle": f"🎉 DAG Execution Complete: {dag_id}",
                "activitySubtitle": f"All tasks finished successfully",
                "facts": [
                    {"name": "DAG", "value": dag_id},
                    {"name": "Execution Date", "value": execution_date.strftime("%Y-%m-%d %H:%M:%S UTC") if execution_date else "N/A"},
                    {"name": "Status", "value": "COMPLETED"},
                    {"name": "Environment", "value": "Production"},
                    {"name": "Notification Type", "value": "TEAMS_WEBHOOK"}
                ],
                "markdown": True
            }
        ]
    }
    
    import requests
    import json
    try:
        from airflow.sdk import Variable
    except ImportError:
        from airflow.models import Variable  # Fallback for older versions
    
    try:
        try:
            teams_webhook_url = Variable.get("teams_webhook_url")
        except Exception as var_error:
            print(f"❌ Failed to get teams_webhook_url variable: {str(var_error)}")
            return
        
        if not teams_webhook_url:
            print("❌ Teams webhook URL not found in Airflow Variables")
            return
        response = requests.post(
            teams_webhook_url,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(summary_message),
            timeout=30
        )
        print("✅ DAG completion notification sent!")
    except Exception as e:
        print(f"❌ Error sending completion notification: {str(e)}")

task_04_completion_notification = PythonOperator(
    task_id='task_04_completion_notification',
    python_callable=dag_completion_notification,
    dag=dag,
)

# Set task dependencies
task_01_start_notification >> task_02_process_data >> task_03_python_processing >> task_04_completion_notification