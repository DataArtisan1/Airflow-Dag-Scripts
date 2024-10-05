from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor

# Function to retrieve result from SubDAG
def retrieve_result(**kwargs):
    ti = kwargs['ti']
    # Pull XCom result from the SubDAG
    subdag_result = ti.xcom_pull(dag_id='SubDag', key='child_result', task_ids='process_task')
    print(f"Result from SubDAG: {subdag_result}")

default_args = {
    'owner': 'YS',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='MasterDag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Trigger the SubDAG to process sensitive data
    trigger_subdag = TriggerDagRunOperator(
        task_id='trigger_subdag',
        trigger_dag_id='SubDag',  # ID of the SubDAG
        conf={'message': 'Customer Data - Process PII'},  # Data to be passed to SubDAG
        execution_date="{{ execution_date }}",  # Ensure matching execution date
        wait_for_completion=True  # Wait for SubDAG to complete
    )

    # Task 2: Use ExternalTaskSensor to wait for a specific task in the SubDAG
    wait_for_subdag = ExternalTaskSensor(
        task_id='wait_for_subdag',
        external_dag_id='SubDag',  # Monitor SubDAG
        external_task_id='process_task',  # Task in SubDAG to wait for
        mode='poke',
        timeout=600,  # Timeout of 10 minutes
    )

    # Task 3: Retrieve result from the SubDAG's XCom (processed PII data)
    retrieve_result_task = PythonOperator(
        task_id='retrieve_result_task',
        python_callable=retrieve_result,
        provide_context=True,  # Pass context to access XCom
    )

    trigger_subdag >> wait_for_subdag >> retrieve_result_task
