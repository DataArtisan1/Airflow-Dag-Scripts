"""
EXAMPLE_12_Dynamic_DAG_Factory Implementation
Author: YS
Description: Production-ready Dynamic DAG Factory for Apache Airflow

Creates multiple Airflow DAGs from a single YAML configuration file using the Factory Pattern.
This implementation generates DAGs dynamically and registers them using globals().
Supports multiple data processing pipelines with configurable transformations.
"""

import yaml
import json
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os
import logging

# Base path for data storage
BASE_PATH = os.path.expanduser("~/airflow/data")

class MultiPipelineDAGFactory:
    """Factory for creating data processing DAGs from YAML configuration"""
    
    def __init__(self, config_path):
        """Initialize factory with configuration file"""
        self.config_path = config_path
        self.config = self._load_config()
        logging.info(f"🏗️ DAG Factory initialized with {len(self.config['pipelines'])} pipeline configurations")
    
    def _load_config(self):
        """Load YAML configuration from file"""
        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
                if 'pipelines' not in config:
                    raise ValueError("Configuration must contain 'pipelines' key")
                logging.info(f"✅ Loaded configuration with {len(config['pipelines'])} pipelines")
                return config
        except FileNotFoundError:
            logging.error(f"❌ Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logging.error(f"❌ Invalid YAML syntax: {e}")
            raise
        except Exception as e:
            logging.error(f"❌ Error loading configuration: {e}")
            raise
    
    def create_dag(self, pipeline_config):
        """Create a complete DAG from pipeline configuration"""
        pipeline_name = pipeline_config['name']
        dag_id = f"EXAMPLE_12_Dynamic_DAG_Factory_{pipeline_name}"
        
        # Default arguments for all tasks in this DAG
        default_args = {
            'owner': 'YS',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
        
        # Create the DAG with configuration from YAML
        dag = DAG(
            dag_id,
            default_args=default_args,
            description=pipeline_config['description'],
            schedule=pipeline_config['schedule'],
            start_date=datetime(2024, 1, 1),
            catchup=False,
            tags=pipeline_config['tags'],
        )
        
        # Create tasks based on configuration
        extract_task = self._create_extract_task(dag, pipeline_config['source'], pipeline_name)
        transform_tasks = self._create_transform_tasks(dag, pipeline_config['transformations'], pipeline_name)
        load_task = self._create_load_task(dag, pipeline_config['destination'], pipeline_name)
        
        # Set up task dependencies: Extract -> Transform(s) -> Load
        if transform_tasks:
            # Chain: extract -> first_transform -> ... -> last_transform -> load
            extract_task >> transform_tasks[0]
            for i in range(len(transform_tasks) - 1):
                transform_tasks[i] >> transform_tasks[i + 1]
            transform_tasks[-1] >> load_task
        else:
            # No transformations: extract -> load
            extract_task >> load_task
        
        logging.info(f"🚀 Created DAG '{dag_id}' with {len(transform_tasks)} transformations")
        return dag
    
    def _create_extract_task(self, dag, source_config, pipeline_name):
        """Create data extraction task based on source configuration"""
        
        def extract_data(**context):
            """Extract data from configured source"""
            url = source_config['file_path']
            logging.info(f"📥 Starting data extraction from: {url}")
            
            try:
                # Download data with timeout
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                # Create pipeline directory using BASE_PATH
                pipeline_dir = os.path.join(BASE_PATH, f"dynamic_dag_factory_{pipeline_name}")
                os.makedirs(pipeline_dir, exist_ok=True)
                
                # Save raw data
                raw_file_path = os.path.join(pipeline_dir, "raw_data.csv")
                with open(raw_file_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                # Load and analyze the data
                df = pd.read_csv(raw_file_path)
                
                # Prepare extraction statistics
                stats = {
                    'pipeline': pipeline_name,
                    'source_url': url,
                    'source_type': source_config['type'],
                    'rows_extracted': len(df),
                    'columns_extracted': len(df.columns),
                    'column_names': list(df.columns),
                    'file_path': raw_file_path,
                    'extraction_timestamp': datetime.now().isoformat(),
                    'file_size_bytes': len(response.content)
                }
                
                # Save extraction metadata
                metadata_file = os.path.join(pipeline_dir, "extraction_metadata.json")
                with open(metadata_file, 'w') as f:
                    json.dump(stats, f, indent=2)
                
                logging.info(f"✅ Extraction completed: {stats['rows_extracted']} rows, {stats['columns_extracted']} columns")
                logging.info(f"📊 Columns: {', '.join(stats['column_names'][:5])}{'...' if len(stats['column_names']) > 5 else ''}")
                
                return stats
                
            except requests.RequestException as e:
                logging.error(f"❌ Network error during extraction: {e}")
                raise
            except pd.errors.EmptyDataError:
                logging.error("❌ Downloaded file is empty or invalid CSV")
                raise
            except Exception as e:
                logging.error(f"❌ Unexpected error during extraction: {e}")
                raise
        
        return PythonOperator(
            task_id=f'extract_{pipeline_name}_data',
            python_callable=extract_data,
            dag=dag,
        )
    
    def _create_transform_tasks(self, dag, transformations, pipeline_name):
        """Create transformation tasks based on configuration"""
        transform_tasks = []
        
        for i, transform_config in enumerate(transformations):
            transform_type = transform_config['type']
            parameters = transform_config.get('parameters', {})
            
            # Get the appropriate transformation function
            transform_function = self._get_transform_function(transform_type)
            if not transform_function:
                logging.warning(f"⚠️ Unknown transformation type: {transform_type}, skipping")
                continue
            
            # Create task with descriptive ID
            task_id = f'transform_{i+1:02d}_{transform_type}_{pipeline_name}'
            
            task = PythonOperator(
                task_id=task_id,
                python_callable=transform_function,
                op_args=[pipeline_name, parameters],
                dag=dag,
            )
            
            transform_tasks.append(task)
            logging.info(f"📝 Created transformation task: {task_id}")
        
        return transform_tasks
    
    def _create_load_task(self, dag, destination_config, pipeline_name):
        """Create data loading task based on destination configuration"""
        
        def load_data(**context):
            """Load processed data to destination"""
            destination_path = destination_config['file_path']
            pipeline_dir = os.path.join(BASE_PATH, f"dynamic_dag_factory_{pipeline_name}")
            processed_file = os.path.join(pipeline_dir, "processed_data.json")
            
            logging.info(f"📤 Starting data load to: {destination_path}")
            
            try:
                # Check if processed data exists
                if not os.path.exists(processed_file):
                    raise FileNotFoundError(f"Processed data file not found: {processed_file}")
                
                # Read processed data
                with open(processed_file, 'r') as f:
                    processed_data = json.load(f)
                
                # Ensure destination directory exists
                destination_dir = os.path.dirname(destination_path)
                if destination_dir:
                    os.makedirs(destination_dir, exist_ok=True)
                
                # Write to final destination
                with open(destination_path, 'w') as f:
                    json.dump(processed_data, f, indent=2)
                
                # Prepare load statistics
                load_stats = {
                    'pipeline': pipeline_name,
                    'destination_path': destination_path,
                    'destination_type': destination_config['type'],
                    'records_loaded': len(processed_data.get('records', [])),
                    'load_timestamp': datetime.now().isoformat(),
                    'file_size_mb': round(os.path.getsize(destination_path) / (1024*1024), 2)
                }
                
                logging.info(f"✅ Load completed: {load_stats['records_loaded']} records saved to {destination_path}")
                logging.info(f"📁 File size: {load_stats['file_size_mb']} MB")
                
                return load_stats
                
            except Exception as e:
                logging.error(f"❌ Error during data load: {e}")
                raise
        
        return PythonOperator(
            task_id=f'load_{pipeline_name}_data',
            python_callable=load_data,
            dag=dag,
        )
    
    def _get_transform_function(self, transform_type):
        """Return appropriate transformation function based on type"""
        transform_functions = {
            'clean_data': self._clean_data,
            'validate': self._validate_data,
            'aggregate': self._aggregate_data,
            'filter': self._filter_data,
            'enrich': self._enrich_data,
        }
        return transform_functions.get(transform_type)
    
    def _prepare_dataframe_for_json(self, df):
        """Convert DataFrame datetime columns to strings for JSON serialization"""
        df_copy = df.copy()
        for col in df_copy.columns:
            if df_copy[col].dtype == 'datetime64[ns]' or hasattr(df_copy[col].dtype, 'tz'):
                df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        return df_copy
    
    # ================================
    # TRANSFORMATION FUNCTIONS
    # ================================
    
    def _clean_data(self, pipeline_name, parameters, **context):
        """Clean data transformation: remove nulls, standardize columns, handle data types"""
        pipeline_dir = os.path.join(BASE_PATH, f"dynamic_dag_factory_{pipeline_name}")
        input_file = os.path.join(pipeline_dir, "raw_data.csv")
        output_file = os.path.join(pipeline_dir, "processed_data.json")
        
        logging.info(f"🧹 Starting data cleaning for {pipeline_name}")
        
        try:
            # Load the raw data
            df = pd.read_csv(input_file)
            initial_rows = len(df)
            initial_columns = list(df.columns)
            
            # Remove null values if specified
            if parameters.get('remove_nulls', False):
                df = df.dropna()
                rows_after_null_removal = len(df)
                logging.info(f"🗑️ Removed null values: {initial_rows} → {rows_after_null_removal} rows")
            
            # Standardize column names if specified
            if parameters.get('standardize_columns', False):
                df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace(r'[^\w]', '', regex=True)
                logging.info(f"📝 Standardized column names: {list(df.columns)}")
            
            # Basic data type optimization
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        # Try to convert to numeric if possible
                        numeric_series = pd.to_numeric(df[col], errors='coerce')
                        if not numeric_series.isna().all():
                            df[col] = numeric_series
                    except:
                        pass
            
            # Convert datetime columns to string format for JSON serialization
            df = self._prepare_dataframe_for_json(df)
            
            # Prepare result with metadata
            result = {
                'pipeline': pipeline_name,
                'transformation': 'clean_data',
                'records': df.to_dict('records'),
                'metadata': {
                    'initial_rows': initial_rows,
                    'final_rows': len(df),
                    'initial_columns': initial_columns,
                    'final_columns': list(df.columns),
                    'rows_removed': initial_rows - len(df),
                    'parameters_applied': parameters,
                    'processing_timestamp': datetime.now().isoformat()
                }
            }
            
            # Save processed data
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            logging.info(f"✅ Data cleaning completed: {len(df)} rows processed")
            return result['metadata']
            
        except Exception as e:
            logging.error(f"❌ Data cleaning failed: {e}")
            raise
    
    def _validate_data(self, pipeline_name, parameters, **context):
        """Data validation transformation: check required columns, validate data types"""
        pipeline_dir = os.path.join(BASE_PATH, f"dynamic_dag_factory_{pipeline_name}")
        input_file = os.path.join(pipeline_dir, "processed_data.json")
        
        # If no processed data exists yet, use raw data
        if not os.path.exists(input_file):
            df = pd.read_csv(os.path.join(pipeline_dir, "raw_data.csv"))
        else:
            with open(input_file, 'r') as f:
                data = json.load(f)
            df = pd.DataFrame(data['records'])
        
        logging.info(f"✅ Starting data validation for {pipeline_name}")
        
        try:
            validation_results = {
                'passed': True,
                'warnings': [],
                'errors': [],
                'info': []
            }
            
            # Check required columns
            required_columns = parameters.get('required_columns', [])
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                validation_results['passed'] = False
                validation_results['errors'].append(f"Missing required columns: {missing_columns}")
                logging.warning(f"⚠️ Missing required columns: {missing_columns}")
            else:
                validation_results['info'].append(f"All required columns present: {required_columns}")
            
            # Validate and convert data types
            data_types = parameters.get('data_types', {})
            for col, expected_type in data_types.items():
                if col in df.columns:
                    try:
                        if expected_type == 'integer':
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        elif expected_type == 'float':
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        elif expected_type == 'string':
                            df[col] = df[col].astype(str)
                        
                        validation_results['info'].append(f"Column '{col}' converted to {expected_type}")
                    except Exception as e:
                        validation_results['warnings'].append(f"Could not convert column '{col}' to {expected_type}: {e}")
                else:
                    validation_results['warnings'].append(f"Column '{col}' not found for type validation")
            
            # Check for data quality issues
            null_counts = df.isnull().sum()
            high_null_columns = null_counts[null_counts > len(df) * 0.5].index.tolist()
            if high_null_columns:
                validation_results['warnings'].append(f"Columns with >50% null values: {high_null_columns}")
            
            # Convert datetime columns to string format for JSON serialization
            df = self._prepare_dataframe_for_json(df)
            
            # Prepare result
            result = {
                'pipeline': pipeline_name,
                'transformation': 'validate',
                'records': df.to_dict('records'),
                'metadata': {
                    'validation_results': validation_results,
                    'rows_validated': len(df),
                    'columns_validated': list(df.columns),
                    'null_counts': null_counts.to_dict(),
                    'parameters_applied': parameters,
                    'processing_timestamp': datetime.now().isoformat()
                }
            }
            
            # Save validated data
            output_file = os.path.join(pipeline_dir, "processed_data.json")
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            status = "✅ PASSED" if validation_results['passed'] else "⚠️ FAILED"
            logging.info(f"{status} Data validation completed for {len(df)} rows")
            
            return result['metadata']
            
        except Exception as e:
            logging.error(f"❌ Data validation failed: {e}")
            raise
    
    def _aggregate_data(self, pipeline_name, parameters, **context):
        """Data aggregation transformation: group data and calculate metrics"""
        pipeline_dir = os.path.join(BASE_PATH, f"dynamic_dag_factory_{pipeline_name}")
        input_file = os.path.join(pipeline_dir, "processed_data.json")
        
        # Load input data
        if not os.path.exists(input_file):
            df = pd.read_csv(os.path.join(pipeline_dir, "raw_data.csv"))
        else:
            with open(input_file, 'r') as f:
                data = json.load(f)
            df = pd.DataFrame(data['records'])
        
        logging.info(f"📊 Starting data aggregation for {pipeline_name}")
        
        try:
            group_by = parameters.get('group_by', [])
            metrics = parameters.get('metrics', [])
            
            initial_rows = len(df)
            
            if group_by and any(col in df.columns for col in group_by):
                # Filter group_by columns that actually exist
                available_group_cols = [col for col in group_by if col in df.columns]
                
                # Start with basic count aggregation
                aggregated_df = df.groupby(available_group_cols).size().reset_index(name='record_count')
                
                # Process metrics
                for metric in metrics:
                    try:
                        if 'sum(' in metric:
                            col = metric.replace('sum(', '').replace(')', '')
                            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                                sum_data = df.groupby(available_group_cols)[col].sum().reset_index()
                                sum_data = sum_data.rename(columns={col: f'sum_{col}'})
                                aggregated_df = aggregated_df.merge(sum_data, on=available_group_cols, how='left')
                        
                        elif 'avg(' in metric:
                            col = metric.replace('avg(', '').replace(')', '')
                            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                                avg_data = df.groupby(available_group_cols)[col].mean().reset_index()
                                avg_data = avg_data.rename(columns={col: f'avg_{col}'})
                                aggregated_df = aggregated_df.merge(avg_data, on=available_group_cols, how='left')
                        
                        elif 'max(' in metric:
                            col = metric.replace('max(', '').replace(')', '')
                            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                                max_data = df.groupby(available_group_cols)[col].max().reset_index()
                                max_data = max_data.rename(columns={col: f'max_{col}'})
                                aggregated_df = aggregated_df.merge(max_data, on=available_group_cols, how='left')
                        
                        elif 'min(' in metric:
                            col = metric.replace('min(', '').replace(')', '')
                            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                                min_data = df.groupby(available_group_cols)[col].min().reset_index()
                                min_data = min_data.rename(columns={col: f'min_{col}'})
                                aggregated_df = aggregated_df.merge(min_data, on=available_group_cols, how='left')
                        
                        elif 'count(' in metric:
                            col = metric.replace('count(', '').replace(')', '')
                            if col in df.columns:
                                count_data = df.groupby(available_group_cols)[col].count().reset_index()
                                count_data = count_data.rename(columns={col: f'count_{col}'})
                                aggregated_df = aggregated_df.merge(count_data, on=available_group_cols, how='left')
                                
                    except Exception as e:
                        logging.warning(f"⚠️ Could not process metric '{metric}': {e}")
                
            else:
                # No grouping possible, create summary statistics
                logging.info("⚠️ No valid grouping columns found, creating summary statistics")
                numeric_cols = df.select_dtypes(include=['number']).columns
                
                summary_data = {}
                for col in numeric_cols:
                    summary_data[f'total_{col}'] = df[col].sum()
                    summary_data[f'avg_{col}'] = df[col].mean()
                    summary_data[f'max_{col}'] = df[col].max()
                    summary_data[f'min_{col}'] = df[col].min()
                
                summary_data['total_records'] = len(df)
                aggregated_df = pd.DataFrame([summary_data])
            
            # Convert datetime columns to string format for JSON serialization
            aggregated_df = self._prepare_dataframe_for_json(aggregated_df)
            
            # Prepare result
            result = {
                'pipeline': pipeline_name,
                'transformation': 'aggregate',
                'records': aggregated_df.to_dict('records'),
                'metadata': {
                    'original_rows': initial_rows,
                    'aggregated_rows': len(aggregated_df),
                    'group_by_columns': group_by,
                    'metrics_applied': metrics,
                    'aggregation_columns': list(aggregated_df.columns),
                    'parameters_applied': parameters,
                    'processing_timestamp': datetime.now().isoformat()
                }
            }
            
            # Save aggregated data
            output_file = os.path.join(pipeline_dir, "processed_data.json")
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            logging.info(f"✅ Data aggregation completed: {initial_rows} → {len(aggregated_df)} rows")
            return result['metadata']
            
        except Exception as e:
            logging.error(f"❌ Data aggregation failed: {e}")
            raise
    
    def _filter_data(self, pipeline_name, parameters, **context):
        """Data filtering transformation: apply conditions to filter datasets"""
        pipeline_dir = os.path.join(BASE_PATH, f"dynamic_dag_factory_{pipeline_name}")
        input_file = os.path.join(pipeline_dir, "processed_data.json")
        
        # Load input data
        if not os.path.exists(input_file):
            df = pd.read_csv(os.path.join(pipeline_dir, "raw_data.csv"))
        else:
            with open(input_file, 'r') as f:
                data = json.load(f)
            df = pd.DataFrame(data['records'])
        
        logging.info(f"🔍 Starting data filtering for {pipeline_name}")
        
        try:
            initial_rows = len(df)
            condition = parameters.get('condition', '')
            
            # Apply filtering based on condition
            if 'Year >= 2010' in condition:
                # Look for Year column
                year_columns = [col for col in df.columns if 'year' in col.lower()]
                if year_columns:
                    year_col = year_columns[0]
                    df[year_col] = pd.to_numeric(df[year_col], errors='coerce')
                    df = df[df[year_col] >= 2010]
                    logging.info(f"📅 Filtered by {year_col} >= 2010")
                elif 'Year' in df.columns:
                    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
                    df = df[df['Year'] >= 2010]
                    logging.info(f"📅 Filtered by Year >= 2010")
                else:
                    logging.warning("⚠️ No Year column found for filtering")
            
            elif '>= 2010' in condition:
                # Generic year filtering
                for col in df.columns:
                    if df[col].dtype in ['int64', 'float64'] or 'year' in col.lower() or 'date' in col.lower():
                        try:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            if df[col].max() > 1900 and df[col].min() > 1900:  # Looks like years
                                df = df[df[col] >= 2010]
                                logging.info(f"📅 Filtered by {col} >= 2010")
                                break
                        except:
                            continue
            
            elif 'Date >=' in condition:
                # Date-based filtering
                date_value = condition.split('>=')[1].strip()
                date_columns = [col for col in df.columns if 'date' in col.lower()]
                if date_columns:
                    date_col = date_columns[0]
                    try:
                        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                        df = df[df[date_col] >= date_value]
                        logging.info(f"📅 Filtered by {date_col} >= {date_value}")
                    except:
                        logging.warning(f"⚠️ Could not parse dates in column {date_col}")
            
            # Remove rows with NaN values that might have been created during filtering
            df = df.dropna()
            
            # Convert datetime columns to string format for JSON serialization
            df = self._prepare_dataframe_for_json(df)
            
            # Prepare result
            result = {
                'pipeline': pipeline_name,
                'transformation': 'filter',
                'records': df.to_dict('records'),
                'metadata': {
                    'initial_rows': initial_rows,
                    'filtered_rows': len(df),
                    'rows_removed': initial_rows - len(df),
                    'filter_condition': condition,
                    'filter_efficiency': round((len(df) / initial_rows) * 100, 2) if initial_rows > 0 else 0,
                    'parameters_applied': parameters,
                    'processing_timestamp': datetime.now().isoformat()
                }
            }
            
            # Save filtered data
            output_file = os.path.join(pipeline_dir, "processed_data.json")
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            logging.info(f"✅ Data filtering completed: {initial_rows} → {len(df)} rows ({result['metadata']['filter_efficiency']}% retained)")
            return result['metadata']
            
        except Exception as e:
            logging.error(f"❌ Data filtering failed: {e}")
            raise
    
    def _enrich_data(self, pipeline_name, parameters, **context):
        """Data enrichment transformation: add timestamps, metadata, calculated fields"""
        pipeline_dir = os.path.join(BASE_PATH, f"dynamic_dag_factory_{pipeline_name}")
        input_file = os.path.join(pipeline_dir, "processed_data.json")
        
        # Load input data
        if not os.path.exists(input_file):
            df = pd.read_csv(os.path.join(pipeline_dir, "raw_data.csv"))
        else:
            with open(input_file, 'r') as f:
                data = json.load(f)
            df = pd.DataFrame(data['records'])
        
        logging.info(f"✨ Starting data enrichment for {pipeline_name}")
        
        try:
            initial_columns = list(df.columns)
            enrichments_applied = []
            
            # Add timestamp if specified
            if parameters.get('add_timestamp', False):
                df['enriched_timestamp'] = datetime.now().isoformat()
                enrichments_applied.append('timestamp')
            
            # Add processing date if specified
            if parameters.get('add_processing_date', False):
                df['processing_date'] = datetime.now().strftime('%Y-%m-%d')
                enrichments_applied.append('processing_date')
            
            # Add category/classification if specified
            if parameters.get('add_category'):
                df['data_category'] = parameters['add_category']
                enrichments_applied.append('category')
            
            # Add pipeline metadata
            df['source_pipeline'] = pipeline_name
            df['enriched_by'] = 'dynamic_dag_factory'
            enrichments_applied.extend(['source_pipeline', 'enriched_by'])
            
            # Add calculated fields based on data
            if 'add_calculated_fields' in parameters and parameters['add_calculated_fields']:
                # Add row ID
                df['row_id'] = range(1, len(df) + 1)
                enrichments_applied.append('row_id')
                
                # Add record hash for deduplication
                df['record_hash'] = df.apply(lambda row: hash(str(row.values)), axis=1)
                enrichments_applied.append('record_hash')
            
            # Add data quality indicators
            df['data_quality_score'] = df.isnull().sum(axis=1).apply(lambda x: max(0, 100 - (x * 10)))
            enrichments_applied.append('data_quality_score')
            
            # Convert datetime columns to string format for JSON serialization
            df = self._prepare_dataframe_for_json(df)
            
            # Prepare result
            result = {
                'pipeline': pipeline_name,
                'transformation': 'enrich',
                'records': df.to_dict('records'),
                'metadata': {
                    'rows_enriched': len(df),
                    'initial_columns': initial_columns,
                    'final_columns': list(df.columns),
                    'columns_added': len(df.columns) - len(initial_columns),
                    'enrichments_applied': enrichments_applied,
                    'parameters_applied': parameters,
                    'processing_timestamp': datetime.now().isoformat()
                }
            }
            
            # Save enriched data
            output_file = os.path.join(pipeline_dir, "processed_data.json")
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            logging.info(f"✅ Data enrichment completed: {len(enrichments_applied)} enrichments applied to {len(df)} rows")
            logging.info(f"🆕 Added columns: {[col for col in df.columns if col not in initial_columns]}")
            
            return result['metadata']
            
        except Exception as e:
            logging.error(f"❌ Data enrichment failed: {e}")
            raise


# ================================
# DAG GENERATION FROM CONFIGURATION
# ================================

# Path to configuration file
config_path = os.path.join(os.path.dirname(__file__), 'configs', 'multi_pipeline_config.yaml')

try:
    # Initialize the DAG factory
    factory = MultiPipelineDAGFactory(config_path)
    
    # Create DAGs for each pipeline configuration and register them using globals()
    created_dags = []
    for pipeline_config in factory.config['pipelines']:
        dag_id = f"dynamic_dag_factory_{pipeline_config['name']}"
        dag = factory.create_dag(pipeline_config)
        
        # Register DAG in globals() so Airflow can discover it
        globals()[dag_id] = dag
        created_dags.append(dag_id)
    
    print(f"🚀 Successfully created {len(created_dags)} dynamic DAGs:")
    for dag_id in created_dags:
        print(f"   📊 {dag_id}")
        
except Exception as e:
    print(f"❌ Error creating dynamic DAGs: {e}")
    
    # Create an error DAG to alert about configuration issues
    error_dag = DAG(
        'dynamic_dag_factory_config_error',
        default_args={
            'owner': 'YS',
            'start_date': datetime(2024, 1, 1),
        },
        description=f'Configuration error in Dynamic DAG Factory: {str(e)}',
        schedule=None,
        catchup=False,
        tags=['error', 'dynamic_dag_factory', 'config_issue']
    )
    
    error_task = EmptyOperator(
        task_id='config_error_notification',
        dag=error_dag
    )
    
    # Register error DAG in globals()
    globals()['dynamic_dag_factory_config_error'] = error_dag
    print("⚠️ Created error DAG: dynamic_dag_factory_config_error")
    print(f"🔧 Please check configuration file: {config_path}")