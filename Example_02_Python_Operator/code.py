from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'YS',  # Owner of the DAG
    'depends_on_past': False,  # Do not wait for previous runs to finish
    'email_on_failure': False,  # Do not email on failure
    'email_on_retry': False,  # Do not email on retry
    'retries': 1,  # Number of retries on failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'Example_02_Python_Operator',  # DAG name
    default_args=default_args,
    description='A simple DAG using PythonOperator functionalities',
    schedule_interval=timedelta(days=1),  # Schedule to run once a day
    start_date=days_ago(1),  # Start date for the DAG
    catchup=False,  # Do not perform backfill
)

# This function demonstrates a simple task without any arguments or return value
def greet():
    print("Hello, Airflow!")

# This function demonstrates the use of PythonOperator with arguments and returns the result to XCom
def add_numbers(a, b):
    result = a + b
    print(f"The sum of {a} and {b} is {result}")
    return result

# This function demonstrates retrieving a value from XCom
def process_result(ti):
    result = ti.xcom_pull(task_ids='add_task')
    print(f"Processing result: {result}")

# This function demonstrates returning a value to be pushed to XCom
def extract_data(**kwargs):
    data = {'value': 42}
    return data

# This function demonstrates retrieving a value from XCom and returning a new value to XCom
def transform_data(ti, **kwargs):
    extracted_data = ti.xcom_pull(task_ids='extract_task')
    transformed_data = extracted_data['value'] * 2
    ti.xcom_push(key='transformed_data', value=transformed_data)
    print(f'Transformed data {transformed_data} sending back.')

# This function demonstrates retrieving a value from XCom and performing an action with it
def load_data(ti, **kwargs):
    transformed_data = ti.xcom_pull(task_ids='transform_task', key='transformed_data')
    print(f"Loading data: {transformed_data}")

# Define the Python tasks using PythonOperator
greet_task = PythonOperator(
    task_id='greet_task',  # Task ID
    python_callable=greet,  # Python function to call
    dag=dag,
)

add_task = PythonOperator(
    task_id='add_task',  # Task ID
    python_callable=add_numbers,  # Python function to call
    op_args=[3, 5],  # Arguments to pass to the function
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_task',  # Task ID
    python_callable=process_result,  # Python function to call
    provide_context=True,  # Provide Airflow context to the function
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_task',  # Task ID
    python_callable=extract_data,  # Python function to call
    provide_context=True,  # Provide Airflow context to the function
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',  # Task ID
    python_callable=transform_data,  # Python function to call
    provide_context=True,  # Provide Airflow context to the function
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',  # Task ID
    python_callable=load_data,  # Python function to call
    provide_context=True,  # Provide Airflow context to the function
    dag=dag,
)

# Define the task dependencies
greet_task >> add_task >> process_task  # greet_task -> add_task -> process_task
extract_task >> transform_task >> load_task  # extract_task -> transform_task -> load_task
