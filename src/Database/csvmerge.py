import csv
import os


def merge_csv(folder_path: str, output_folder_path: str) -> None:
  """
  Merges CSV files from a specified folder into a single CSV file per date.

  The function reads all CSV files from the input folder, groups them by date,
  and writes the merged content into new CSV files in the output folder.

  The date is extracted from the filename, assuming the format is consistent.
  @param folder_path: The path to the folder containing the input CSV files.
  @param output_folder_path: The path to the folder where the merged CSV files will be saved.
  @return: None
  """
  # List all files in the input folder
  file_list = os.listdir(folder_path)
  csv_dict = {}

  # Group files by date extracted from the filename
  for file_name in file_list:
    if file_name.endswith('.csv'):
      # Extract year, month, and day from the filename
      parts = file_name.split('_')
      date_key = '_'.join(parts[3:6])  # e.g., '2024_11_14'
      if date_key not in csv_dict:
        csv_dict[date_key] = []
      csv_dict[date_key].append(file_name)

  # Merge files for each date and write to the output folder
  for date_key, files in csv_dict.items():
    with open(f"{output_folder_path}/merged_{date_key}.csv", 'w', newline='') as merged_file:
      writer = csv.writer(merged_file)
      header_written = False
      for file_name in files:
        with open(f"{folder_path}/{file_name}", 'r') as csv_file:
          reader = csv.reader(csv_file)
          header = next(reader)
          if not header_written:
            # Write header only once for each date file
            writer.writerow(header)
            header_written = True
          for row in reader:
            writer.writerow(row)

if __name__ == "__main__":
  if os.name == 'nt':
    folder_path = "E:\\Developer\\SourceRepo\\GFW-Research\\src\\Lib\\Data-2024-11-12\\China-Mobile\\DNSPoisoning"
    output_folder_path = "E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\Data-2024-11\\ChinaMobile"
  else:
    folder_path = "/Users/silverhand/Developer/SourceRepo/GFW-Research/src/Lib/Data-2024-11-12/China-Mobile/DNSPoisoning"
    output_folder_path = "/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile"
  merge_csv(folder_path, output_folder_path)
