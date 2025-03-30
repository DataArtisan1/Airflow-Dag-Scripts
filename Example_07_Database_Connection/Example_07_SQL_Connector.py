from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
import logging

def process_mysql_results(**context):
    """Process results from MySQL query stored in XCom"""
    try:
        ti = context['task_instance']
        results = ti.xcom_pull(task_ids='query_mysql_tables')
        
        if not results:
            logging.warning("No results found")
            return {"status": "no_data"}
            
        for row in results:
            # Access tuple elements by index since MySqlOperator returns tuples
            table_name, row_count = row[0], row[1]
            logging.info(f"Table: {table_name}, Rows: {row_count}")
        
        return {"status": "success", "count": len(results)}
    except Exception as e:
        logging.error(f"Error processing results: {e}")
        raise

with DAG(
    'EXAMPLE_08_MYSQL_Operator_Connection',
    default_args={
        'owner': 'YS',
        'start_date': days_ago(1),
        'retries': 1,
    },
    description='MySQL Operator DAG',
    schedule_interval=None,
    catchup=False
) as dag:

    query_tables = MySqlOperator(
        task_id='query_mysql_tables',
        mysql_conn_id='my_mysql_conn',
        sql="""
            SELECT TABLE_NAME, TABLE_ROWS 
            FROM information_schema.tables 
            WHERE TABLE_SCHEMA = 'airflow'
            ORDER BY TABLE_NAME;
        """
    )

    process_results = PythonOperator(
        task_id='process_results',
        python_callable=process_mysql_results,
        provide_context=True
    )

    query_tables >> process_results