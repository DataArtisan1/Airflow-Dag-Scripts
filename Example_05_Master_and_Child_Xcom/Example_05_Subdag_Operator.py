from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the default arguments for the DAG and sub-DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the main DAG
main_dag = DAG(
    dag_id='Example_05_Subdag_Operator',
    default_args=default_args,
    description='Example DAG demonstrating SubDagOperator',
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Define a function to create sub-DAGs
def create_subdag(parent_dag_name, child_dag_name, args):
    # Define the sub-DAG with the correct dag_id format
    subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',  # Use the correct child_dag_name
        default_args=args,
        schedule_interval="@daily",
        start_date=args['start_date'],  # Make sure to pass the start_date
    )

    # Define two PythonOperator tasks within the sub-DAG
    with subdag:
        start = PythonOperator(
            task_id='start',
            python_callable=lambda: print(f"SubDAG {child_dag_name}: Start"),
        )

        end = PythonOperator(
            task_id='end',
            python_callable=lambda: print(f"SubDAG {child_dag_name}: End"),
        )

        # Set task dependencies within the sub-DAG
        start >> end

    return subdag

# Define PythonOperator tasks to print start and end messages for the main DAG
start = PythonOperator(
    task_id='start',
    python_callable=lambda: print("Main DAG: Start"),
    dag=main_dag,
)

# Define the SubDagOperator
subdag_task = SubDagOperator(
    task_id='subdag_task',
    subdag=create_subdag('Example_05_Subdag_Operator', 'subdag_task', {**default_args, 'start_date': main_dag.start_date}),
    dag=main_dag,
)

end = PythonOperator(
    task_id='end',
    python_callable=lambda: print("Main DAG: End"),
    dag=main_dag,
)

# Set task dependencies for the main DAG
start >> subdag_task >> end
