from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun
from airflow.utils.session import create_session
from datetime import datetime, timezone

# Define the DAGs and their respective thresholds in seconds
DAG_THRESHOLDS = {
    'dag_a': 10,   # 10 seconds
    'dag_b': 20,   # 20 seconds
    'dag_c': 30,   # 30 seconds
}

def list_long_running_dags(**kwargs):
    long_running_dags = []

    # Use create_session to manage session scope
    with create_session() as session:
        # Query all running DAG runs
        for dag_run in session.query(DagRun).filter(DagRun.state == 'running').all():
            execution_time = dag_run.execution_date
            duration = (datetime.now(timezone.utc) - execution_time).total_seconds()  # Duration in seconds

            # Check if the DAG ID is in the defined thresholds
            threshold = DAG_THRESHOLDS.get(dag_run.dag_id)
            if threshold and duration > threshold:
                long_running_dags.append({
                    'dag_id': dag_run.dag_id,
                    'execution_date': dag_run.execution_date,
                    'duration': duration,
                    'threshold': threshold
                })

    if long_running_dags:
        print("Long Running DAGs:")
        for dag in long_running_dags:
            print(f"DAG ID: {dag['dag_id']}, Execution Date: {dag['execution_date']}, "
                  f"Duration: {dag['duration']} seconds, Threshold: {dag['threshold']} seconds")
    else:
        print("No long running DAGs found.")

default_args = {
    'owner': 'YS',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='Example_06_list_long_running_dags',
    default_args=default_args,
    schedule_interval='@hourly',  # Adjust as needed
    catchup=False,
) as dag:

    list_long_running_dags_task = PythonOperator(
        task_id='list_long_running_dags',
        python_callable=list_long_running_dags,
        provide_context=True,  # Ensure the context is provided
    )

    list_long_running_dags_task
