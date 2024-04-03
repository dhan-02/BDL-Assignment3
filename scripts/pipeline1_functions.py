from random import sample
import os
import shutil

def select_file_rand(datafile_path, number_files):
    """
    Selects a random sample of files from the html file containing the list of files

    Args:
        datafile_path (str): Path to the html file containing the links
        number_files (int): Number of files to select randomly.

    Returns:
        list: A list of selected file names.
    """
    # Validate input arguments
    if not isinstance(datafile_path, str) or not os.path.isfile(datafile_path):
        raise ValueError("Invalid datafile_path provided.")

    if not isinstance(number_files, int) or number_files <= 0:
        raise ValueError("number_files should be a positive integer.")

    # Read datafile and extract relevant file names
    with open(datafile_path, "r") as page:
        data_links = []
        for line in page:
            # Check whether we are looking at a line of interest
            if "href" in line and ".csv" in line:
                # Extract filename from the link
                filename = line.split("  ")[0].split('"')[1]
                data_links.append(filename)
        # Check if enough files are available for sampling
        if len(data_links) < number_files:
            raise ValueError("Insufficient number of files in the datafile_path.")
        # Sample number_files files from the list of all valid files
        selected_files = sample(data_links, number_files)
        return selected_files

def fetch_ind_files(**kwargs):
    """
    Fetches individual files based on selected file names and a base URL.

    Args:
        **kwargs: Arbitrary keyword arguments.
            base_url (str): Base URL for data download
            current_dir (str): Current directory.
            ti (airflow.models.TaskInstance): Task instance object.

    Returns:
        None
    """
    # Retrieve keyword arguments
    ti = kwargs['ti']
    base_url = kwargs.get('base_url')
    current_dir = kwargs.get("current_dir")

    # Validate keyword arguments
    if not isinstance(base_url, str):
        raise ValueError("base_url should be a string.")

    if not isinstance(current_dir, str):
        raise ValueError("current_dir should be a string.")

    # Access the selected files that were returned by the select_files function
    selected_files = ti.xcom_pull(task_ids='select_files')

    # Create a new folder where we can download all csv files
    directory = "selected_files"
    path = os.path.join(current_dir, directory)
    os.mkdir(path)

    # Iterate through selected files and download
    for filename in selected_files:
        download_url = f"{base_url}{filename}"
        fetch_command = f"wget {download_url} -P {path} -q"
        os.system(fetch_command)

def zip_files(**kwargs):
    """
    Function to zip the selected downloaded files.

    Args:
        **kwargs: Arbitrary keyword arguments.
            directory (str): Directory to be zipped.
            current_dir (str): Current directory.

    Returns:
        str: Path to the created zip file.
    """
    # Retrieve keyword arguments
    ti = kwargs['ti']

    # Directory is the folder containing all the csv files
    directory = kwargs.get('directory')  # Get directory from task parameters
    current_dir = kwargs.get('current_dir')

    # Validate keyword arguments
    if not isinstance(directory, str):
        raise ValueError("directory should be a string.")

    if not isinstance(current_dir, str):
        raise ValueError("current_dir should be a string.")

    if not os.path.exists(directory):
        raise ValueError("Directory doesn't exist.")

    # Create an archive of the folder
    shutil.make_archive(directory, 'zip', directory)
    shutil.rmtree(directory)  # Delete the original folder

    # Also remove the initial html file that was used to select files as it is no longer needed
    os.remove(os.path.join(current_dir, "data_store.html"))
    return directory

def move_zip_file(**kwargs):
    """
    Moves the zip file to a new location.

    Args:
        **kwargs: Arbitrary keyword arguments.
            new_location (str): New location to move the zip file.
            current_dir (str): Current directory.

    Returns:
        None
    """
    # Retrieve keyword arguments
    new_location = kwargs.get('new_location')
    current_dir = kwargs.get('current_dir')

    # Validate keyword arguments
    if not isinstance(new_location, str):
        raise ValueError("new_location should be a string.")

    if not isinstance(current_dir, str):
        raise ValueError("current_dir should be a string.")

    # Define the path to the zip file
    zip_file_cur_path = os.path.join(current_dir, 'selected_files.zip')
    if not os.path.exists(zip_file_cur_path):
        raise FileNotFoundError("Selected zip file doesn't exist.")

    # Move the zip file to the new location
    shutil.move(zip_file_cur_path, os.path.join(new_location, os.path.basename(zip_file_cur_path)))
