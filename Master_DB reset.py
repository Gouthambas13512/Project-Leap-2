import csv

# Path to the source file (from which headers are copied)
source_file_path = 'output7.csv'
# Path to the destination file (to which headers are written)
destination_file_path = 'Master_DB.csv'

# Open the source file in read mode and the destination file in write mode
with open(source_file_path, mode='r', newline='', encoding='utf-8') as source_file, \
     open(destination_file_path, mode='w', newline='', encoding='utf-8') as dest_file:
    # Create a CSV reader to read the source file
    reader = csv.reader(source_file)
    # Create a CSV writer to write to the destination file
    writer = csv.writer(dest_file)
    
    # Read the first row from the source file (headers)
    headers = next(reader, None)
    
    # Write the headers to the destination file
    if headers:
        writer.writerow(headers)

