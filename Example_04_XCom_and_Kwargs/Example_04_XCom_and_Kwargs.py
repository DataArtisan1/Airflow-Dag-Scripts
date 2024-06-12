from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'YS',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'Example_04_XCom_and_Kwargs',
    default_args=default_args,
    description='A simple DAG demonstrating the usage of XCom and kwargs',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# This function decides which branch to take based on the input
def choose_branch(**kwargs):
    # Use kwargs to access the execution date
    execution_date = kwargs['execution_date']
    print(f"Execution date is {execution_date}")

    # Based on some condition, decide which task to follow
    return 'branch_a' if kwargs['params']['condition'] else 'branch_b'

# This function processes the branch chosen by the BranchPythonOperator
def process_branch(**kwargs):
    branch_data = kwargs['task_instance'].xcom_pull(task_ids='branch_task')
    print(f"Processing branch data: {branch_data}")

# Define the BranchPythonOperator
branch_task = PythonOperator(
    task_id='branch_task',
    python_callable=choose_branch,
    provide_context=True,
    params={'condition': True},  # Change this condition to False to follow branch_b
    dag=dag,
)

# Define the PythonOperator for processing the chosen branch
process_branch_task = PythonOperator(
    task_id='process_branch_task',
    python_callable=process_branch,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
branch_task >> process_branch_task
