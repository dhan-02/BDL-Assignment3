import csv
import ast

def text_to_csv(input_file, output_file,req_fields):
    with open(input_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
        csv_writer = csv.writer(f_out)
        header = ['LATITUDE','LONGITUDE','MONTH'] + req_fields
        csv_writer.writerow(header)

        while True:
            # Read latitude line
            latitude_line = f_in.readline().strip()
            if not latitude_line:
                break

            # Read longitude line
            longitude_line = f_in.readline().strip()

            # Read 2D array of averages
            averages_array_str = f_in.readline().strip()
            # Replace 'nan' with a placeholder value
            averages_array_str = averages_array_str.replace('nan', 'None')
            averages_array = ast.literal_eval(averages_array_str)

            # Write data to CSV
            for month, row in enumerate(averages_array, start=1):
                # Replace 'None' with 'nan'
                row = [float(val) if val is not None else 'nan' for val in row]
                csv_writer.writerow([latitude_line, longitude_line, month] + row)


if __name__ == '__main__':
    input_file = '/home/dhan/Desktop/CS5820-Assignments/Assignment2/scripts/Assignment2.txt' # Specify the path to your input text file
    output_file = '/home/dhan/Desktop/CS5820-Assignments/Assignment2/scripts/Assignment2.csv'  # Specify the path to your output CSV file

    text_to_csv(input_file, output_file)
