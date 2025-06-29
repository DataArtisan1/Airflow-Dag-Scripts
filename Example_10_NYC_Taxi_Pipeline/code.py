"""
This file defines an Airflow DAG for processing NYC Yellow Taxi data and generating a report.
Compatible with both old and new Airflow versions.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Handle version compatibility for dates
try:
    from airflow.utils.dates import days_ago
except ImportError:
    # For newer Airflow versions (2.2+)
    from datetime import datetime, timedelta
    def days_ago(n):
        return datetime.now() - timedelta(days=n)

# Handle version compatibility for Variable import
try:
    from airflow.models import Variable
except ImportError:
    try:
        from airflow.sdk import Variable
    except ImportError:
        from airflow.models import Variable

from datetime import timedelta, datetime
import pandas as pd
import json
import os
import logging

logger = logging.getLogger(__name__)

# Configuration variables
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
# Use home directory instead of /tmp to avoid permission issues
BASE_PATH = os.path.expanduser("~/airflow/data")
DATA_PATH = f"{BASE_PATH}/yellow_taxi_data.parquet"
SUMMARY_PATH = f"{BASE_PATH}/taxi_summary.json"
REPORT_PATH = f"{BASE_PATH}/taxi_report.txt"

# Default arguments for the DAG
default_args = {
    'owner': 'YS',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creating the DAG with appropriate configurations
# Handle both old and new Airflow DAG creation syntax
try:
    # New Airflow syntax (2.0+)
    dag = DAG(
        "EXAMPLE_10_NYC_Taxi_Pipeline",
        default_args=default_args,
        description="A simple pipeline for NYC Taxi data analysis",
        schedule=timedelta(days=1),  # New parameter name
        start_date=days_ago(1),
        catchup=False
    )
except TypeError:
    # Fallback for older Airflow versions
    dag = DAG(
        "EXAMPLE_10_NYC_Taxi_Pipeline",
        default_args=default_args,
        description="A simple pipeline for NYC Taxi data analysis",
        schedule_interval=timedelta(days=1),  # Old parameter name
        start_date=days_ago(1),
        catchup=False
    )

# Task 1: Download Data
download_data = BashOperator(
    task_id='download_data',
    bash_command=f"""
    mkdir -p {BASE_PATH} && \
    curl -k -o {DATA_PATH} {DATA_URL} && \
    ls -la {DATA_PATH} && \
    echo "File size: $(du -h {DATA_PATH} | cut -f1)"
    """,
    dag=dag,
)

# Task 2: Analyze Data
def analyze_taxi_data(**kwargs):
    """Analyze taxi data and generate statistics"""
    
    # Debug: Check file existence and size
    logger.info(f"Looking for data file at: {DATA_PATH}")
    if os.path.exists(DATA_PATH):
        file_size = os.path.getsize(DATA_PATH)
        logger.info(f"File found! Size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")
    else:
        logger.error(f"Data file not found at {DATA_PATH}")
        # List directory contents for debugging
        dir_path = os.path.dirname(DATA_PATH)
        if os.path.exists(dir_path):
            files = os.listdir(dir_path)
            logger.info(f"Files in {dir_path}: {files}")
        raise FileNotFoundError(f"Data file not found at {DATA_PATH}")

    try:
        # Read the parquet file
        df = pd.read_parquet(DATA_PATH)
        logger.info(f"Successfully loaded parquet file with shape: {df.shape}")
        
        # Calculate basic statistics
        stats = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'file_size_mb': round(os.path.getsize(DATA_PATH) / 1024 / 1024, 2),
            'passenger_count': {
                'mean': float(df['passenger_count'].mean()),
                'min': float(df['passenger_count'].min()),
                'max': float(df['passenger_count'].max())
            },
            'trip_distance': {
                'mean': float(df['trip_distance'].mean()),
                'min': float(df['trip_distance'].min()),
                'max': float(df['trip_distance'].max())
            },
            'fare_amount': {
                'mean': float(df['fare_amount'].mean()),
                'min': float(df['fare_amount'].min()),
                'max': float(df['fare_amount'].max())
            }
        }

        # Save results to JSON
        with open(SUMMARY_PATH, 'w') as f:
            json.dump(stats, f, indent=2)

        logger.info(f"Analysis complete. Processed {stats['row_count']:,} records.")
        return stats
        
    except Exception as e:
        logger.error(f"Error analyzing data: {str(e)}")
        raise

# Handle different PythonOperator syntax for different versions
try:
    # New Airflow syntax
    analyze_data = PythonOperator(
        task_id='analyze_data',
        python_callable=analyze_taxi_data,
        dag=dag,
    )
except TypeError:
    # Fallback for older versions
    analyze_data = PythonOperator(
        task_id='analyze_data',
        python_callable=analyze_taxi_data,
        provide_context=True,
        dag=dag,
    )

# Task 3: Generate Report
def generate_report(**kwargs):
    """Generate a formatted report from analysis results"""
    
    try:
        # Read the analysis results
        with open(SUMMARY_PATH, 'r') as f:
            stats = json.load(f)

        # Create formatted report
        report = f"""
==============================================
NYC YELLOW TAXI DATA ANALYSIS REPORT
==============================================
Source: {DATA_URL}
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

DATASET OVERVIEW:
----------------
Total Records: {stats['row_count']:,}
Total Columns: {stats['column_count']}

STATISTICS:
----------------
1. Passenger Count:
   - Mean: {stats['passenger_count']['mean']:.2f}
   - Min:  {stats['passenger_count']['min']}
   - Max:  {stats['passenger_count']['max']}

2. Trip Distance:
   - Mean: {stats['trip_distance']['mean']:.2f} miles
   - Min:  {stats['trip_distance']['min']:.2f} miles
   - Max:  {stats['trip_distance']['max']:.2f} miles

3. Fare Amount:
   - Mean: ${stats['fare_amount']['mean']:.2f}
   - Min:  ${stats['fare_amount']['min']:.2f}
   - Max:  ${stats['fare_amount']['max']:.2f}

SUMMARY:
----------------
This report analyzes NYC Yellow Taxi data for January 2023.
The dataset contains information about {stats['row_count']:,} taxi trips.

==============================================
"""

        # Save report to file
        with open(REPORT_PATH, 'w') as f:
            f.write(report)

        logger.info("Report generated successfully")
        print(report)  # Also print to logs
        
        return {"status": "success", "report_path": REPORT_PATH}
        
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        raise

# Handle different PythonOperator syntax
try:
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        dag=dag,
    )
except TypeError:
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
        dag=dag,
    )

# Task 4: Cleanup
cleanup = BashOperator(
    task_id='cleanup',
    bash_command=f'rm -f {DATA_PATH} {SUMMARY_PATH} && echo "Cleanup completed"',
    dag=dag,
)

# Set task dependencies
download_data >> analyze_data >> generate_report_task >> cleanup