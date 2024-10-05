from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import time

# Function to simulate a long-running task
def long_running_task(duration):
    print(f"Task running for {duration} seconds...")
    time.sleep(duration)
    print("Task completed.")

# Default arguments for the DAGs
default_args = {
    'owner': 'YS',
    'start_date': days_ago(1),
}

# DAG A: Simulates a task that runs for 1 hour
with DAG(
    dag_id='dag_a',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag_a:
    start_task_a = DummyOperator(task_id='start')
    
    long_task_a = PythonOperator(
        task_id='long_task',
        python_callable=long_running_task,
        op_kwargs={'duration': 3600}  # Simulate a task running for 3600 seconds (1 hour)
    )
    
    end_task_a = DummyOperator(task_id='end')

    start_task_a >> long_task_a >> end_task_a


# DAG B: Simulates a task that runs for 2 hours
with DAG(
    dag_id='dag_b',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag_b:
    start_task_b = DummyOperator(task_id='start')

    long_task_b = PythonOperator(
        task_id='long_task',
        python_callable=long_running_task,
        op_kwargs={'duration': 7200}  # Simulate a task running for 7200 seconds (2 hours)
    )
    
    end_task_b = DummyOperator(task_id='end')

    start_task_b >> long_task_b >> end_task_b


# DAG C: Simulates a task that runs for 30 minutes
with DAG(
    dag_id='dag_c',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag_c:
    start_task_c = DummyOperator(task_id='start')

    long_task_c = PythonOperator(
        task_id='long_task',
        python_callable=long_running_task,
        op_kwargs={'duration': 1800}  # Simulate a task running for 1800 seconds (30 minutes)
    )
    
    end_task_c = DummyOperator(task_id='end')

    start_task_c >> long_task_c >> end_task_c
