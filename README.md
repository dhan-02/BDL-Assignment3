# BDL-Assignment3
Repository for the 3rd Assignment of BIg Data Lab - 8th semester (Jan-May 24). This is nothing but simply git and version control practise for the 2nd Assignment of the same course. i.e this assignment just involves putting the 2nd assignment on git.

## Instructions on how to run
The repository consists of a scripts folder and a data folder. The dependencies are all stored in requirements.txt. <br />
Use conda create --name my_env --file requirements.txt to create an environment with required libraries. 
### Starting airflow
Make sure that these commands are run from the base directory (repository) folder
airflow webserver -p 8080 <br />
airflow scheduler <br />
The interface can be then opened up in the corresponding local host. Note that the dags can be paused and triggered in the interface itself. 
![dhanpic](https://github.com/dhan-02/BDL-Assignment3/assets/74642765/b82515e2-accd-4599-a7e9-b0c7db071c99)
<br />
The tasks success and failures and schedules can also be monitored in the interface
<br />
![dhanpic2](https://github.com/dhan-02/BDL-Assignment3/assets/74642765/c13979e8-7df0-4622-8da5-dc249cf846eb)

## Problem Statement
Pipeline 1 <br />
1. Fetch the page containing the location wise datasets for that year. (Bash Operator with
wget or curl command) 
2. Based on the required number of data files, select the data files randomly from the available
list of files. (Python Operator)
3. Fetch the individual data files (Bash or Python Operator)
4. Zip them into an archive. (Python Operator)
5. Place the archive at a required location. (Bash/Python Operator)

Pipeline 2 <br />
1. Wait for the archive to be available (with a timeout of 5 secs) at the destined location. If
the wait has timed out, stop the pipeline. (FileSensor)
2. Upon the availability (status=success), check if the file is a valid archive followed by unzip
the contents into individual CSV files. (BashOperator)
3. Extract the contents of the CSV into a data frame and filter the dataframe based on the
required fields such Windspeed or BulbTemperature, etc. Extract also the Lat/Long values
from the CSV to create a tuple (PythonOperator using Apache Beam)
4. Setup another PythonOperator over ApacheBeam to compute the monthly averages of the
required fields.
5. Using ‘geopandas’ and ‘geodatasets’ packages, create a visualization where you plot the
required fields (one per field) using heatmaps at different lat/lon positions.(PythonOperator using ApacheBeam)
6. Upon successful completion, delete the CSV file from the destined location.

## Script structure and content
Pipeline 1 involves downloading required number of files and zipping to a particular location. <br />
Pipeline 2 involves computing monthly averages and then using those to plot on a geomap. <br />
pipeline1_dag.py and pipeline2_dag.py are the dag definition files whereas pipeline1_functions.py and pipeline2_functions.py contain the required function definitions. <br /> <br />
The dependency order of tasks in the pipeline are as shown below: <br />
fetch_task >> select_task >> fetch_ind_task >> zip_task >> move_zip_task for pipeline1 and <br />
wait_for_archive >> check_unzip >> process_csv >> visualize >> delete_files for pipeline2 <br />

## Outputs
Folders selected_files and extracted_files are created during run time which contain the required files.<br />
The folder heatmap_visualizatios contains all the plots pertaining to the heatmaps of monthly values across locations of each field for each month
