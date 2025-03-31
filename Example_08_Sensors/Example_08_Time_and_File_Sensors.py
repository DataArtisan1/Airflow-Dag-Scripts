"""
Example demonstrating File Sensor Pattern with proper connection handling.
This example will not immediately move to the next task after the FileSensor succeeds.
Instead, it will wait for a specified time before checking for files.
"""
from datetime import timedelta, datetime

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.time_sensor import TimeSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.utils.timezone import utcnow
from datetime import timedelta, datetime
import logging
import glob
import os
import json
import time

def process_file_info(**context):
    """Process information from FileSensor results"""
    try:
        # Get connection details
        conn = BaseHook.get_connection('file_system_default')
        base_path = json.loads(conn.extra).get('path', '/root/airflow/data')
        
        # Retrieve the list of files matching the pattern
        sensor_files = glob.glob(os.path.join(base_path, '*.csv'))
        
        logging.info(f"Processing sensor results from: {base_path}")
        logging.info(f"Sensor found files: {sensor_files}")
        
        file_details = []
        if sensor_files:
            for filepath in sensor_files:
                file_stat = os.stat(filepath)
                file_details.append({
                    'name': os.path.basename(filepath),
                    'size': file_stat.st_size,
                    'modified': datetime.fromtimestamp(file_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                    'path': filepath
                })
                logging.info(f"Processed file details: {filepath}")
        
        context['task_instance'].xcom_push(key='file_details', value=file_details)
        return {
            "status": "success" if file_details else "no_files",
            "file_count": len(file_details),
            "processed_time": context['execution_date'].isoformat(),
            "base_path": base_path
        }
    except Exception as e:
        logging.error(f"Error processing sensor results: {e}")
        raise

def create_summary(**context):
    """Create summary of processed files"""
    try:
        ti = context['task_instance']
        file_info = ti.xcom_pull(task_ids='process_files')
        file_details = ti.xcom_pull(key='file_details', task_ids='process_files')
        
        summary = [
            "File Processing Summary",
            "=====================",
            f"Execution Date: {file_info['processed_time']}",
            f"Status: {file_info['status']}",
            f"Base Path: {file_info['base_path']}",
            f"Total Files Found: {file_info['file_count']}",
            "\nFile Details:",
            "============="
        ]
        
        if file_details:
            for file_detail in file_details:
                summary.extend([
                    f"\nFile: {file_detail['name']}",
                    f"Size: {file_detail['size']} bytes",
                    f"Last Modified: {file_detail['modified']}",
                    f"Full Path: {file_detail['path']}",
                    "-" * 50
                ])
        else:
            summary.append("\nNo files were processed in this run.")
        
        summary_path = '/root/airflow/data/processing_summary.txt'
        with open(summary_path, 'w') as f:
            f.write('\n'.join(summary))
            
        logging.info(f"Summary written to: {summary_path}")
        return {"summary_path": summary_path}
    except Exception as e:
        logging.error(f"Error creating summary: {e}")
        raise

def decide_next_step(**context):
    """Decide whether to process files or skip processing"""
    ti = TaskInstance(context['dag'].get_task('wait_for_files'), context['execution_date'])
    ti.refresh_from_db()  # Refresh task instance state from the database

    if ti.state == State.SKIPPED:  # Task was skipped due to timeout
        logging.info("FileSensor timed out. Skipping file processing.")
        return 'skip_processing'
    elif ti.state == State.SUCCESS:  # Task succeeded (files found)
        logging.info("FileSensor found files. Proceeding to process files.")
        return 'process_files'
    else:
        logging.error(f"Unexpected state for FileSensor: {ti.state}")
        raise ValueError(f"Unexpected state for FileSensor: {ti.state}")

def calculate_target_time():
    """Calculate the target time 5 minutes from now."""
    now = utcnow()
    target_time = (now + timedelta(minutes=5)).time()
    logging.info(f"Calculated target time: {target_time}")
    return target_time

with DAG(
    'EXAMPLE_08_TIME_FILE_SENSOR',
    default_args={
        'owner': 'YS',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'start_date': days_ago(1),
    },
    description='Time and File Sensor Example',
    schedule_interval=None,
    catchup=False,
    tags=['example', 'sensor', 'files']
) as dag:

    # Task 1: Wait for 5 minutes
    wait_for_time = TimeSensor(
        task_id='wait_for_time',
        target_time=calculate_target_time(),  # Ensure this returns a valid datetime.time object
        doc_md="""
        ### Time Sensor Task
        Waits for 5 minutes before proceeding to check for files.
        """
    )

    # Task 2: Wait for CSV files
    wait_for_files = FileSensor(
        task_id='wait_for_files',
        fs_conn_id='file_system_default',
        filepath='*.csv',
        poke_interval=1,      # Minimum valid value: 1 second
        timeout=300,          # 5 minutes timeout
        mode='reschedule',    # Free up worker slot between checks
        soft_fail=True,       # Continue DAG even if timeout
        doc_md="""
        ### File Sensor Task
        Monitors for CSV files in the configured directory.
        * Waits up to 5 minutes
        * Checks every minute
        * Soft fail if no files found
        * Uses reschedule mode to free up worker slot
        * Uses connection ID `file_system_default` to get the base path
        * File pattern: `*.csv`
        * If files are found, it will trigger the next task
        * If no files are found, it will soft fail and continue the DAG
        """
    )

    # Task 3: Decide next step
    decide_next = BranchPythonOperator(
        task_id='decide_next',
        python_callable=decide_next_step,
        provide_context=True,
        doc_md="""
        ### Decision Task
        Decides whether to process files or skip processing based on FileSensor results.
        """
    )

    # Task 4: Process found files
    process_files = PythonOperator(
        task_id='process_files',
        python_callable=process_file_info,
        provide_context=True,
        doc_md="""
        ### File Processing Task
        Processes files found by sensor and collects details.
        """
    )

    # Task 5: Skip processing
    skip_processing = BashOperator(
        task_id='skip_processing',
        bash_command='echo "No files found. Skipping processing."',
        doc_md="""
        ### Skip Processing Task
        Logs a message indicating no files were found.
        """
    )

    # Task 6: Create summary report
    create_file_summary = PythonOperator(
        task_id='create_file_summary',
        python_callable=create_summary,
        provide_context=True,
        doc_md="""
        ### Summary Creation Task
        Generates detailed report of processed files.
        """
    )

    # Task 7: Cleanup old files
    cleanup_files = BashOperator(
        task_id='cleanup_files',
        bash_command='find {{ ti.xcom_pull(task_ids="process_files")["base_path"] }} -name "*.csv" -mtime +7 -delete || true',
        doc_md="""
        ### Cleanup Task
        Removes CSV files older than 7 days.
        """
    )

    # Set task dependencies
    wait_for_time >> wait_for_files >> decide_next
    decide_next >> process_files >> create_file_summary >> cleanup_files
    decide_next >> skip_processing