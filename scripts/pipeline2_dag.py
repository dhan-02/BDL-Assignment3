from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta,timezone
import os
from pipeline2_functions import *

# Get the current directory
current_dir = os.getcwd()

# The below variable should be set to the same value as new_location in pipeline1_dag.py
# Make sure that there is no / after the last subfolder
zip_loc = '/home/dhan/Desktop/CS5820-Assignments/Assignment2/data'
# Specify which fields to visualize
hm_fields = ['HourlyWindSpeed','HourlyWindDirection']
# Define default arguments for the DAG
default_args = {
    'owner': 'Dhanush',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 28),  # Set the start date for the DAG
    'email': ['ch20b036@smail.iitm.ac.in'],
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
dag = DAG(
    'Pipeline2_DAG_Dhanush',
    default_args=default_args,
    description='Data Processing Pipeline',
    schedule=timedelta(minutes=2),  # Run the DAG every 2 minute
    catchup=False,  # Don't run any missed schedules
    max_active_runs=1  # Limit the number of active DAG runs to 1
)

# Sensor to wait for the archive file to be available
wait_for_archive = FileSensor(
    task_id='wait_for_archive',
    poke_interval=1,  # Check every 1 second
    timeout=5,  # Timeout after 5 seconds
    filepath=zip_loc,  # Specify the path where the zip file is expected
    dag=dag
)

# Bash operator to check and unzip the archive file
check_unzip = BashOperator(
    task_id='check_unzip',
    bash_command=(
        'mkdir -p {1}/extracted_data '  # Create a directory for extracted data
        '&& unzip -t {0}/selected_files.zip '  # Test the zip file
        '&& unzip {0}/selected_files.zip -d {1}/extracted_data'  # Unzip the files to the extracted_data directory
    ).format(zip_loc, current_dir),
    dag=dag
)

# Python operator to process the extracted CSV files
process_csv = PythonOperator(
    task_id='extract_and_findavg',
    python_callable=filter_extract_beam,  # Python function to call
    op_kwargs={'all_files_path': os.path.join(current_dir, 'extracted_data'), 'req_fields': req_fields,'current_dir':current_dir},  # Pass necessary arguments
    provide_context=True,  # Provide task instance context
    dag=dag,
)

visualize = PythonOperator(
    task_id='visualize_heatmaps',
    python_callable=visualize_beam,  # Python function to call
    op_kwargs={'current_dir':current_dir,'req_fields': req_fields,'hm_fields':hm_fields},  # Pass necessary arguments
    provide_context=True,  # Provide task instance context
    dag=dag,
)

# Bash operator to delete the extracted CSV files
delete_files = BashOperator(
    task_id="delete_csv_files",
    bash_command='rm -r {0}/extracted_data'.format(current_dir),
    dag=dag
)

# Define the task dependencies
wait_for_archive >> check_unzip >> process_csv >> visualize >> delete_files