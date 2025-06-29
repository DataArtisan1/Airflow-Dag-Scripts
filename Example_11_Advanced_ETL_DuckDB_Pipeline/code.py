"""
This file defines an advanced Airflow DAG for processing NYC Yellow Taxi data with DuckDB storage.
Built for modern Airflow versions (2.5+).
"""

# Airflow imports
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

# Standard library imports
from datetime import timedelta, datetime
import pandas as pd
import json
import os
import logging
import sys
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG Configuration variables
DAG_ID = "EXAMPLE_11_Advanced_ETL_DuckDB_Pipeline"
SCHEDULE_INTERVAL = timedelta(days=1)
START_DATE = datetime(2025, 6, 7)

# File paths - Use home directory to avoid permission issues
BASE_PATH = os.path.expanduser("~/airflow/data")
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
DOWNLOAD_PATH = f"{BASE_PATH}/yellow_taxi_data.parquet"
SUMMARY_PATH = f"{BASE_PATH}/taxi_summary.json"
REPORT_PATH = f"{BASE_PATH}/taxi_report.md"
DB_PATH = f"{BASE_PATH}/taxi_analysis.db"
LOG_PATH = f"{BASE_PATH}/taxi_processing.log"

# Required packages
REQUIRED_PACKAGES = ["pandas", "pyarrow", "duckdb"]

# Expected columns for validation
EXPECTED_COLUMNS = [
    'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 
    'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
    'improvement_surcharge', 'total_amount'
]

# Default arguments for the DAG
default_args = {
    'owner': 'YS',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creating the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="Advanced ETL pipeline for NYC Taxi data analysis with DuckDB storage",
    schedule=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False,
)

# Helper function to setup logging
def setup_logging():
    """Setup detailed logging for the pipeline"""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        
        # Configure file handler
        file_handler = logging.FileHandler(LOG_PATH, mode='a')
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # Get logger and add handler
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        logger.addHandler(file_handler)

        return logger
    except Exception as e:
        print(f"Logging setup error: {e}")
        # Return a basic logger even if file setup fails
        return logging.getLogger(__name__)

# Task 1: Create Directories
def create_directories(**kwargs):
    """Create necessary directories for the pipeline"""
    logger = setup_logging()

    logger.info(f"Pipeline started at {datetime.now()}")

    directories = [BASE_PATH]

    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}") 
        else:
            logger.info(f"Directory already exists: {directory}")

    logger.info("Directories created successfully")  
    return {"status": "success", "base_path": BASE_PATH}

create_directories_task = PythonOperator(
    task_id='create_directories',
    python_callable=create_directories,
    dag=dag,
)

# Task 2: Install Dependencies
def install_dependencies(**kwargs):
    """Dynamically install required dependencies"""
    logger = setup_logging()

    logger.info(f"Attempting to install packages: {REQUIRED_PACKAGES}")

    for package in REQUIRED_PACKAGES:
        try:
            __import__(package)
            logger.info(f"Package {package} already installed")
        except ImportError:
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])
                success_msg = f"Successfully installed {package}"
                logger.info(success_msg)
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to install {package}: {e}")
                raise

    logger.info("All dependencies verified/installed")
    return {"status": "success", "packages": REQUIRED_PACKAGES}

install_dependencies_task = PythonOperator(
    task_id='install_dependencies',
    python_callable=install_dependencies,
    dag=dag,
)

# Task 3: Download Parquet File
download_parquet = BashOperator(
    task_id='download_parquet',
    bash_command=f"""
    mkdir -p {BASE_PATH} && \
    curl -k -o {DOWNLOAD_PATH} {DATA_URL} && \
    ls -la {DOWNLOAD_PATH} && \
    echo "Download completed successfully. File size: $(du -h {DOWNLOAD_PATH} | cut -f1)"
    """,
    dag=dag,
)

# Task 4: Check File Exists
check_file_exists = FileSensor(
    task_id='check_file_exists',
    filepath=DOWNLOAD_PATH,
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag,
)

# Task 5: Create DuckDB Table
def create_duckdb_table(**kwargs):
    """Create DuckDB database and table structure"""
    logger = setup_logging()

    try:
        import duckdb

        logger.info("Creating DuckDB connection and table")
        conn = duckdb.connect(DB_PATH)

        # Drop table if exists to recreate with correct schema
        conn.execute("DROP TABLE IF EXISTS taxi_analysis_results")
        logger.info("Dropped existing table if it existed")

        # Create table for storing analysis results with auto-incrementing id
        conn.execute("""
            CREATE TABLE taxi_analysis_results (
                id INTEGER PRIMARY KEY,
                analysis_date TIMESTAMP,
                row_count INTEGER,
                vendor_counts JSON,
                passenger_stats JSON,
                distance_stats JSON,
                fare_stats JSON )
        """)
        logger.info("DuckDB table created successfully")
        conn.close()
        
        return {"status": "success", "db_path": DB_PATH}

    except Exception as e:
        logger.error(f"Error creating DuckDB table: {str(e)}")
        raise

create_duckdb_table_task = PythonOperator(
    task_id='create_duckdb_table',
    python_callable=create_duckdb_table,
    dag=dag,
)

# Task 6: Validate Parquet (BranchPythonOperator)
def validate_parquet(**kwargs):
    """Validate Parquet file structure and data quality"""
    logger = setup_logging()

    try:
        logger.info("Starting parquet validation")

        if not os.path.exists(DOWNLOAD_PATH):
            logger.error(f"Parquet file not found at {DOWNLOAD_PATH}")
            return 'skip_analysis'

        df = pd.read_parquet(DOWNLOAD_PATH)
        logger.info(f"Successfully read parquet file with {len(df)} rows")

        # Check for expected columns
        missing_columns = [col for col in EXPECTED_COLUMNS if col not in df.columns]
        if missing_columns:
            logger.warning(f"Missing expected columns: {missing_columns}")

        if len(df) == 0:
            logger.error("DataFrame is empty")
            return 'skip_analysis'

        # Check for critical columns
        critical_columns = ['passenger_count', 'trip_distance', 'fare_amount', 'VendorID']
        for col in critical_columns:
            if col not in df.columns:
                logger.error(f"Critical column missing: {col}")
                return 'skip_analysis'

        # Check data types and missing values
        null_counts = df[critical_columns].isnull().sum()
        logger.info(f"Null value counts: {null_counts.to_dict()}")

        logger.info("Parquet validation passed")
        return 'analyze_data'

    except Exception as e:
        logger.error(f"Error during parquet validation: {str(e)}")
        return 'skip_analysis'

validate_parquet_task = BranchPythonOperator(
    task_id='validate_parquet',
    python_callable=validate_parquet,
    dag=dag,
)

# Task 7: Analyze Data
def analyze_data(**kwargs):
    """Perform comprehensive data analysis"""
    logger = setup_logging()

    try:
        df = pd.read_parquet(DOWNLOAD_PATH)
        logger.info(f"Analyzing {len(df)} records")

        # Calculate vendor distribution
        vendor_counts = df['VendorID'].value_counts().to_dict()
        vendor_counts = {str(k): int(v) for k, v in vendor_counts.items()}

        # Calculate statistics for passenger count
        passenger_stats = {
            'mean': float(df['passenger_count'].mean()),
            'min': int(df['passenger_count'].min()),
            'max': int(df['passenger_count'].max())
        }

        # Calculate statistics for trip distance
        distance_stats = {
            'mean': float(df['trip_distance'].mean()),
            'min': float(df['trip_distance'].min()),
            'max': float(df['trip_distance'].max())
        }

        # Calculate statistics for fare amount
        fare_stats = {
            'mean': float(df['fare_amount'].mean()),
            'min': float(df['fare_amount'].min()),
            'max': float(df['fare_amount'].max())
        }

        # Compile all results
        analysis_results = {
            'row_count': len(df),
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'vendor_counts': vendor_counts,
            'passenger_stats': passenger_stats,
            'distance_stats': distance_stats,
            'fare_stats': fare_stats
        }

        # Save results to JSON
        with open(SUMMARY_PATH, 'w') as f:
            json.dump(analysis_results, f, indent=2)

        logger.info("Data analysis completed successfully")
        logger.info(f"Analysis summary: {analysis_results}")

        return analysis_results

    except Exception as e:
        logger.error(f"Error during data analysis: {str(e)}")
        raise

analyze_data_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
)

# Task 8: Skip Analysis (Empty task for failed validation)
skip_analysis = EmptyOperator(
    task_id='skip_analysis',
    dag=dag,
)

# Task 9: Store in DuckDB
def store_in_duckdb(**kwargs):
    """Store analysis results in DuckDB"""
    logger = setup_logging()

    try:
        import duckdb

        logger.info("Starting DuckDB storage process")

        # Read analysis results
        with open(SUMMARY_PATH, 'r') as f:
            results = json.load(f)

        logger.info("Analysis results loaded from JSON file")

        # Connect to DuckDB
        conn = duckdb.connect(DB_PATH)

        # Get next ID value
        next_id_result = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM taxi_analysis_results").fetchone()
        next_id = next_id_result[0]

        logger.info(f"Inserting record with ID: {next_id}")

        # Insert results into table with explicit ID
        conn.execute("""
            INSERT INTO taxi_analysis_results
            (id, analysis_date, row_count, vendor_counts, passenger_stats, distance_stats, fare_stats)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            next_id,
            results['analysis_date'],
            results['row_count'],
            json.dumps(results['vendor_counts']),
            json.dumps(results['passenger_stats']),
            json.dumps(results['distance_stats']),
            json.dumps(results['fare_stats'])
        ))

        # Verify insertion
        result = conn.execute("SELECT COUNT(*) FROM taxi_analysis_results").fetchone()
        logger.info(f"Total records in DuckDB: {result[0]}")

        conn.close()
        logger.info("Results stored in DuckDB successfully")
        
        return {"status": "success", "record_id": next_id}

    except Exception as e:
        logger.error(f"Error storing results in DuckDB: {str(e)}")
        raise

store_in_duckdb_task = PythonOperator(
    task_id='store_in_duckdb',
    python_callable=store_in_duckdb,
    dag=dag,
)

# Task 10: Generate Report
def generate_report(**kwargs):
    """Generate a comprehensive Markdown report"""
    logger = setup_logging()

    try:
        logger.info("Starting report generation")

        # Read analysis results
        with open(SUMMARY_PATH, 'r') as f:
            stats = json.load(f)

        logger.info("Analysis results loaded for report generation")

        # Create formatted Markdown report
        report = f"""# NYC Yellow Taxi Data Analysis Report
Generated on: {stats['analysis_date']}

## Dataset Overview
- Source: NYC Yellow Taxi Trip Data
- Format: Parquet
- Total Records: {stats['row_count']:,}

## Key Statistics

### Vendor Distribution
```json
{json.dumps(stats['vendor_counts'], indent=2)}
```

### Passenger Count
- Average: {stats['passenger_stats']['mean']:.2f}
- Minimum: {stats['passenger_stats']['min']}
- Maximum: {stats['passenger_stats']['max']}

### Trip Distance (miles)
- Average: {stats['distance_stats']['mean']:.2f}
- Minimum: {stats['distance_stats']['min']:.2f}
- Maximum: {stats['distance_stats']['max']:.2f}

### Fare Amount ($)
- Average: {stats['fare_stats']['mean']:.2f}
- Minimum: {stats['fare_stats']['min']:.2f}
- Maximum: {stats['fare_stats']['max']:.2f}

## Data Storage
- Local DuckDB: {DB_PATH}
- Analysis Results: {SUMMARY_PATH}
- Processing Log: {LOG_PATH}

## Summary
This report analyzes NYC Yellow Taxi data with {stats['row_count']:,} trip records.
The data shows an average trip distance of {stats['distance_stats']['mean']:.2f} miles
with an average fare of ${stats['fare_stats']['mean']:.2f}.

The majority of trips are handled by vendor {max(stats['vendor_counts'], key=stats['vendor_counts'].get)}
with {stats['vendor_counts'][max(stats['vendor_counts'], key=stats['vendor_counts'].get)]:,} trips.

## Pipeline Execution
- Pipeline executed successfully on Apache Airflow
- All validation checks passed
- Data stored in DuckDB for further analysis
- Advanced ETL patterns implemented by YS
"""

        # Save report to file
        with open(REPORT_PATH, 'w') as f:
            f.write(report)

        logger.info("Report generated successfully")
        logger.info(f"Report saved to: {REPORT_PATH}")

        return {"status": "success", "report_path": REPORT_PATH}

    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        raise

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# Task 11: Cleanup Files
def cleanup_files(**kwargs):
    """Clean up temporary files"""
    logger = setup_logging()

    logger.info("Starting cleanup process")

    # Files to keep for verification
    keep_files = [REPORT_PATH, SUMMARY_PATH, DB_PATH, LOG_PATH]

    # Files to remove
    cleanup_files_list = [DOWNLOAD_PATH]

    for file_path in cleanup_files_list:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Removed temporary file: {file_path}")
            else:
                logger.info(f"File {file_path} does not exist - already cleaned up")
        except Exception as e:
            logger.error(f"Could not remove file {file_path}: {str(e)}")

    logger.info("Cleanup completed")
    logger.info(f"Kept files for verification: {keep_files}")
    
    return {"status": "success", "kept_files": keep_files}

cleanup_files_task = PythonOperator(
    task_id='cleanup_files',
    python_callable=cleanup_files,
    dag=dag,
    trigger_rule='one_success'  # Fixed trigger rule
)

# Set task dependencies
create_directories_task >> install_dependencies_task >> download_parquet >> check_file_exists >> create_duckdb_table_task >> validate_parquet_task

# Branching: validation passes -> analyze_data, validation fails -> skip_analysis
validate_parquet_task >> [analyze_data_task, skip_analysis]

# Main path when validation passes
analyze_data_task >> store_in_duckdb_task >> generate_report_task >> cleanup_files_task

# Alternative path when validation fails (skip to cleanup)
skip_analysis >> cleanup_files_task