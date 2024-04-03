import apache_beam as beam
import os
import pandas as pd
import numpy as np
import ast
import csv
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt

# Define the default index fields
default_fields = ["LATITUDE","LONGITUDE","DATE"]
# Define the required fields for extraction here 
req_fields = ['HourlyDewPointTemperature','HourlyDryBulbTemperature','HourlyPrecipitation',
'HourlyPressureChange','HourlyPressureTendency','HourlyRelativeHumidity', 'HourlyStationPressure',
# 'HourlySkyConditions', # 'HourlyVisibility',
'HourlySeaLevelPressure','HourlyWetBulbTemperature','HourlyWindDirection','HourlyWindSpeed']

# Define a custom DoFn for filtering and extracting data from CSV files
class FilterExtractCSV(beam.DoFn):
    """
    Custom DoFn to filter and extract data from CSV files.
    """
    def process(self, file_path, **kwargs):
        """
        Processes each file and extracts the required data.

        Args:
            file_path (str): Path to the CSV file.
            **kwargs: Additional keyword arguments.

        Returns:
            list: Tuple containing extracted data in the format <Lat, Lon, [[ArrayOfHourlyDataOfTheReqFields]]>
        """
        # Get the required fields
        req_fields = kwargs['req_fields']

        # Validate input arguments
        if not isinstance(file_path, str) or not os.path.isfile(file_path):
            raise ValueError("Invalid file_path provided.")

        if not isinstance(req_fields, list) or not all(isinstance(field, str) for field in req_fields):
            raise ValueError("Invalid req_fields provided.")

        # Read the CSV file into a dataframe
        df = pd.read_csv(file_path, low_memory=False)
        df['MONTH'] = df['DATE'].apply(lambda date: int(date.split('-')[1]))
        df['YEAR'] = df['DATE'].apply(lambda date: int(date.split('-')[0]))
        df.drop(columns=['DATE'], inplace=True)
        
        # Clean and convert fields
        for field in req_fields:
            df[field] = pd.to_numeric(df[field].astype(str), errors='coerce')
        
        default_fields = ['LATITUDE', 'LONGITUDE', 'MONTH', 'YEAR']
        df = df[default_fields + req_fields] 
        
        # Group by 'LATITUDE' and 'LONGITUDE' and construct the required tuple structure
        grouped_data = df.groupby(['LATITUDE', 'LONGITUDE']).apply(lambda x: x[['MONTH'] + req_fields].values.tolist()).reset_index(name='data')
        
        # Construct the final tuples
        result = [(row['LATITUDE'], row['LONGITUDE'], row['data']) for index, row in grouped_data.iterrows()]  
        return result
    
# Define a custom DoFn for computing monthly averages
class GetMonthlyAverage(beam.DoFn):
    """
    Custom DoFn to compute monthly averages.
    """
    def process(self,element,**kwargs):
        """
        Processes each element to compute monthly averages.

        Args:
            element (tuple): Tuple containing latitude, longitude, and data array.
            **kwargs: Additional keyword arguments.

        Returns:
            tuple: Tuple containing latitude, longitude, and monthly averages.
        """
        lat = element[0]
        long = element[1]
        data_array = np.array(element[2])

        # Extract months and fields from data array
        months = data_array[:, 0]
        fields = data_array[:, 1:]
        
        # Validate input arguments
        if not isinstance(lat, (int, float)):
            raise ValueError("Latitude should be a number.")

        if not isinstance(long, (int, float)):
            raise ValueError("Longitude should be a number.")

        if not isinstance(data_array, np.ndarray):
            raise ValueError("Invalid data array.")

        # Initialize arrays to store sums and counts for each month and field
        month_sums = np.zeros((12, fields.shape[1]))
        month_counts = np.zeros((12, fields.shape[1]), dtype=int)
        
        # Accumulate sums and counts for each month and field
        for month in range(1, 13):
            month_mask = months == month
            month_data = fields[month_mask]
            # Caluclate mean ignoring nan values
            month_sums[month - 1] = np.nanmean(month_data, axis=0) * np.sum(~np.isnan(month_data), axis=0)
            # Count non-NaN values
            month_counts[month - 1] = np.sum(~np.isnan(month_data), axis=0)  

        # Compute averages where counts are non-zero
        averages = np.where(month_counts!=0,month_sums/month_counts,np.nan)
        return (lat,long,averages.tolist())

# Function to list CSV files in a folder
def list_csv_files(folder_path):
    """
    Lists CSV files in a given folder path.

    Args:
        folder_path (str): Path to the folder containing CSV files.

    Returns:
        list: List of paths to CSV files.
    """
    csv_files = []

    # Validate input argument
    if not isinstance(folder_path, str) or not os.path.isdir(folder_path):
        raise ValueError("Invalid folder_path provided.")

    # Iterate through subdirectories and files in the given folder
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            # Check if the file is csv, then append
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files

# Main function for filtering and extracting data using Apache Beam
def filter_extract_beam(**kwargs):
    """
    Main function for filtering and extracting data using Apache Beam.

    Args:
        **kwargs: Additional keyword arguments.

    Returns:
        None
    """
    #Get the all files and path and the req fields from the given keyword arguments 
    all_files_path = kwargs.get('all_files_path')
    req_fields = kwargs.get('req_fields')
    current_dir = kwargs.get('current_dir')

    # Validate input arguments
    # if not isinstance(all_files_path, str) or not os.path.isdir(all_files_path):
    #     raise ValueError("Invalid all_files_path provided.")

    if not isinstance(req_fields, list) or not all(isinstance(field, str) for field in req_fields):
        raise ValueError("Invalid req_fields provided.")

    # Get list of csv files present in the folder
    csv_files = list_csv_files(all_files_path)

    # Build a Beam Pipeline
    with beam.Pipeline() as pipeline:
        processed = (
                pipeline
                | 'Set up Files' >> beam.Create(csv_files)
                | 'Filter and Extract' >> beam.ParDo(FilterExtractCSV(), req_fields=req_fields)
            )

        
        monthly_average = (
            processed
            | 'Compute Monthly Average' >> beam.ParDo(GetMonthlyAverage())
            | 'Write to Text' >> beam.io.WriteToText(os.path.join(current_dir,'temp'), file_name_suffix='.txt', shard_name_template='',)
        )
    pipeline.run()


# Below Functions are for task5 purely.
def texttocsv(**kwargs):
    """
    Convert text data to CSV format.

    Parameters:
        kwargs (dict): Keyword arguments containing input_file, output_file, and req_fields.

    Returns:
        None
    """
    # Validate input arguments
    if 'input_file' not in kwargs or 'output_file' not in kwargs or 'req_fields' not in kwargs:
        raise ValueError("Missing required arguments: input_file, output_file, req_fields")

    input_file = kwargs['input_file']
    output_file = kwargs['output_file']
    req_fields = kwargs['req_fields']

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file '{input_file}' not found.")

    # Open input and output files
    with open(input_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
        csv_writer = csv.writer(f_out)

        # Write header to the CSV file
        header = ['LATITUDE', 'LONGITUDE', 'MONTH'] + req_fields
        csv_writer.writerow(header)

        # Loop through the input text file
        while True:
            # Read latitude line
            latitude_line = f_in.readline().strip()
            if not latitude_line:
                break  # Break if end of file is reached

            # Read longitude line
            longitude_line = f_in.readline().strip()

            # Read 2D array of averages
            averages_array_str = f_in.readline().strip()
            # Replace 'nan' with a placeholder value
            averages_array_str = averages_array_str.replace('nan', 'None')
            averages_array = ast.literal_eval(averages_array_str)

            # Write data to CSV for each month
            for month, row in enumerate(averages_array, start=1):
                # Replace 'None' with 'nan'
                row = [float(val) if val is not None else 'nan' for val in row]
                csv_writer.writerow([latitude_line, longitude_line, month] + row)

def make_heatmap_plots(gdf, field):
    """
    Generate heatmap visualizations for a given field.

    Parameters:
        gdf (GeoDataFrame): GeoDataFrame containing data.
        field (str): Field for which heatmap needs to be generated.

    Returns:
        None
    """
    # Validate input arguments
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise ValueError("Input 'gdf' must be a GeoDataFrame.")
    if field not in gdf.columns:
        raise ValueError(f"Field '{field}' not found in GeoDataFrame.")

    # Group data by latitude, longitude, and month, and calculate the mean
    grouped_data = gdf.groupby(['LATITUDE', 'LONGITUDE', 'MONTH'])[field].mean().reset_index()

    # Create a folder for saving visualizations for each field
    output_folder = os.path.join('heatmap_visualizations', field)
    os.makedirs(output_folder, exist_ok=True)

    # Generate visualization for each month
    for (latitude, longitude, month), group in grouped_data.groupby(['LATITUDE', 'LONGITUDE', 'MONTH']):
        month_year = f"{month:02d}"

        # Create plot
        fig, ax = plt.subplots(1, 1, figsize=(12, 10))
        group.plot(ax=ax, kind='scatter', x='LONGITUDE', y='LATITUDE', c=field, cmap='YlOrRd', legend=True,
                   s=50, edgecolor='black', linewidth=0.5)
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        world.plot(ax=ax, color='lightgray', edgecolor='black')
        ax.set_title(f'Heatmap: {field} - {month_year}')
        ax.set_axis_off()

        # Save the plot in the specified folder structure
        output_file_path = os.path.join(output_folder, f"{month_year}.png")
        plt.savefig(output_file_path, bbox_inches='tight')
        plt.close()

def visualize_beam(**kwargs):
    """
    Visualize data using Apache Beam pipeline.

    Parameters:
        kwargs (dict): Keyword arguments containing current_dir and req_fields.

    Returns:
        None
    """
    # Validate input arguments
    if 'current_dir' not in kwargs or 'req_fields' not in kwargs:
        raise ValueError("Missing required arguments: current_dir, req_fields")

    current_dir = kwargs['current_dir']
    req_fields = kwargs['req_fields']
    hm_fields = kwargs['hm_fields']
    # Define paths for temporary files
    temp_txt_path = os.path.join(current_dir, 'temp.txt')
    temp_csv_path = os.path.join(current_dir, 'temp.csv')

    # Generate CSV from text data
    texttocsv(input_file=temp_txt_path, output_file=temp_csv_path, req_fields=req_fields)

    # Create heatmap visualizations
    df = pd.read_csv(temp_csv_path)
    df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
    df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')
    geometry = [Point(xy) for xy in zip(df['LONGITUDE'], df['LATITUDE'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    for field in hm_fields:
        make_heatmap_plots(gdf, field)

    # Remove temporary files
    os.remove(temp_txt_path)
    os.remove(temp_csv_path)