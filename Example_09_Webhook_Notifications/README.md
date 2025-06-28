# Webhook Notifications

Example Airflow DAG demonstrating Microsoft Teams webhook notifications integration.

## Prerequisites

* Airflow 2.0+
* Microsoft Teams with webhook configured
* Python `requests` library (usually included with Airflow)

## Setup

### 1. Configure Teams Webhook

1. Go to your Teams channel
2. Click **⋯** → **Connectors** → **Incoming Webhook**
3. Configure and copy the webhook URL

### 2. Set Airflow Variable

```bash
# CLI method
airflow variables set teams_webhook_url "YOUR_WEBHOOK_URL_HERE"

# Or via Airflow UI: Admin → Variables → Add Variable
# Key: teams_webhook_url
# Value: [your webhook URL]
```

### 3. Deploy DAG

Copy `EXAMPLE_09_teams_notifications.py` to your Airflow `dags/` folder.

## What It Does

The DAG demonstrates:

* **Start notifications** - When DAG begins
* **Task success/failure callbacks** - Individual task notifications
* **Completion notifications** - Final summary

## Task Flow

```
task_01_start_notification 
    ↓
task_02_process_data 
    ↓
task_03_python_processing 
    ↓
task_04_completion_notification
```

## Teams Notifications

You'll receive messages for:

* 🚀 DAG Started
* ✅ Task Success
* ❌ Task Failure (with Airflow link)
* 🎉 DAG Complete

## Troubleshooting

**Error: `teams_webhook_url` variable not found**

* Set the variable using the setup instructions above

**Error: `execution_date` KeyError**

* This DAG handles both old and new Airflow versions automatically

**Teams not receiving messages**

* Verify webhook URL is correct
* Check Airflow task logs for error details
* Test webhook manually with curl

## Customization

* Modify `send_teams_notification()` function for custom message formats
* Update Airflow URL in failure notifications (line with `localhost:8080`)
* Adjust colors and emojis in `theme_colors` dictionary
