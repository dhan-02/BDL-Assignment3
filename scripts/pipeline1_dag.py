from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta, timezone
from pipeline1_functions import *
import os
import logging 

# Get current directory
current_dir = os.getcwd()

# Define input parameters - These can be modified accordingly

year = '2000'  # Specify the year for data retrieval
new_location = '/home/dhan/Desktop/CS5820-Assignments/Assignment2/data'  # Specify the new location to move the files
no_of_files = 2  # Specify the number of files to select randomly

if not (1901 <= int(year) <= 2024):
    raise ValueError("Invalid year. Year should be between 1901 and 2024.")

if no_of_files > 10:
    logging.warning("Handling a large number of files. Be cautious!")

# Check if the new location exists and is valid
if not os.path.exists(new_location):
    raise FileNotFoundError("The specified new location does not exist or is invalid.")

# Define default arguments for the DAG
default_args = {
    'owner': 'Dhanush',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 28),  # Set the start date for the DAG
    'email': ['ch20b036@smail.iitm.ac.in'], 
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the Airflow DAG
dag = DAG(
    'Pipeline1_DAG_Dhanush',  # Specify the DAG ID
    default_args=default_args,  # Set default arguments
    description='Data Extraction pipeline',  # Description of the DAG
    schedule=timedelta(minutes=4),  # Set the schedule interval
    catchup=False,  # Disable backfilling
    max_active_runs=1
)

# Define tasks in the DAG using operators
# Task to fetch the full data
fetch_task = BashOperator(
    task_id="fetch_full",
    bash_command="wget -O {0}/data_store.html https://www.ncei.noaa.gov/data/local-climatological-data/access/{1}".format(current_dir, year),
    dag=dag
)

# Task to select random files from the fetched data
select_task = PythonOperator(
    task_id='select_files',
    python_callable=select_file_rand,
    op_kwargs={"datafile_path": "data_store.html", "number_files": no_of_files},  # Pass necessary arguments
    dag=dag,
)

# Task to fetch individual files based on selection
fetch_ind_task = PythonOperator(
    task_id="fetch_individual_files",
    python_callable=fetch_ind_files,
    op_kwargs={'base_url': "https://www.ncei.noaa.gov/data/local-climatological-data/access/" + year + "/", 'current_dir': current_dir},
    provide_context=True,  # Provide task instance context
    dag=dag,
)

# Task to zip selected files
zip_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    op_kwargs={'directory': os.path.join(current_dir, 'selected_files'), 'current_dir': current_dir},  # Pass necessary arguments
    provide_context=True,  # Provide task instance context
    dag=dag
)

# Task to move zipped files to a new location
move_zip_task = PythonOperator(
    task_id='move_zip_file',
    python_callable=move_zip_file,
    op_kwargs={'new_location': new_location, 'current_dir': current_dir},  # Pass necessary arguments
    provide_context=True,  # Provide task instance context
    dag=dag,
)

# Define task dependencies
fetch_task >> select_task >> fetch_ind_task >> zip_task >> move_zip_task
