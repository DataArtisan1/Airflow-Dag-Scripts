from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
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
    'Example_03_Branching_and_Dummy',
    default_args=default_args,
    description='A simple DAG demonstrating BranchPythonOperator and DummyOperator',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# This function decides which branch to take based on the input
def choose_branch(**kwargs):
    # This is where your branching logic will go
    # For example, based on some external condition or input, decide which task to follow
    # Returning the task_id of the next task to follow
    return 'branch_a' if kwargs['params']['condition'] else 'branch_b'

# Define the BranchPythonOperator
branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=choose_branch,
    provide_context=True,
    params={'condition': True},  # Change this condition to False to follow branch_b
    dag=dag,
)

# Define DummyOperators for branching
branch_a = DummyOperator(
    task_id='branch_a',
    dag=dag,
)

branch_b = DummyOperator(
    task_id='branch_b',
    dag=dag,
)

# Define a downstream task for both branches
follow_up_task = DummyOperator(
    task_id='follow_up_task',
    dag=dag,
    trigger_rule='one_success',  # Trigger only if at least one upstream task has succeeded
)

# Set the branching dependencies
branch_task >> branch_a >> follow_up_task
branch_task >> branch_b >> follow_up_task
