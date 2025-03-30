from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import mysql.connector
from mysql.connector import Error
from typing import Dict, Any
import logging

def test_mysql_connection(**context) -> Dict[str, Any]:
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='airflow',
            user='root',
            password='password',
            port=3306
        )

        if connection.is_connected():
            results = {}
            
            # Get server info
            db_info = connection.get_server_info()
            logging.info(f"Connected to MySQL Server version {db_info}")
            results['server_version'] = db_info
            
            cursor = connection.cursor(dictionary=True)
            
            # Get database version
            cursor.execute("SELECT VERSION();")
            record = cursor.fetchone()
            logging.info(f"Database version: {record['VERSION()']}")
            results['db_version'] = record['VERSION()']

            # Get table counts
            cursor.execute("""
                SELECT TABLE_NAME, TABLE_ROWS 
                FROM information_schema.tables 
                WHERE TABLE_SCHEMA = 'airflow'
                ORDER BY TABLE_NAME;
            """)
            tables = cursor.fetchall()
            results['tables'] = tables
            
            logging.info("\nTable Statistics:")
            for table in tables:
                logging.info(f"Table: {table['TABLE_NAME']}, Rows: {table['TABLE_ROWS']}")

            return results

    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        raise
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            logging.info("MySQL connection closed")

# Define default arguments
default_args = {
    'owner': 'YS',
    'start_date': days_ago(1),
    'retries': 1,
}

# Create DAG
with DAG(
    'EXAMPLE_07_MYSQL_PYTHON_CONNECTION',
    default_view='tree',
    default_args=default_args,
    description='Test MySQL Connection DAG',
    schedule_interval=None,
    catchup=False
) as dag:

    test_connection = PythonOperator(
        task_id='test_mysql_connection',
        python_callable=test_mysql_connection,
    )

    # Set task dependencies if you add more tasks
    test_connection