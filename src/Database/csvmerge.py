import csv
import os

def merge_csv(folder_paths: list, output_folder_path: str) -> None:
  """
  Merges CSV files from specified folders into a single CSV file per date.

  The function reads all CSV files from the input folders, groups them by date,
  and writes the merged content into new CSV files in the output folder.

  The date is extracted from the filename, assuming the format is consistent.
  @param folder_paths: A list of paths to the folders containing the input CSV files.
  @param output_folder_path: The path to the folder where the merged CSV files will be saved.
  @return: None
  """
  csv_dict = {}

  # Group files by date extracted from the filename
  for folder_path in folder_paths:
    file_list = os.listdir(folder_path)
    for file_name in file_list:
      if file_name.endswith('.csv'):
        # Extract year, month, and day from the filename
        parts = file_name.split('_')
        if len(parts) == 7:  # Format: DNS_Checking_Result_YYYY_MM_DD_HH_MM.csv
          date_key = '_'.join(parts[3:6])  # e.g., '2024_11_17'
        elif len(parts) == 6:  # Format: DNS_Checking_Result_YYYY_MM_DD.csv
          date_key = '_'.join(parts[3:6])  # e.g., '2024_11_17'
        else:
          continue
        if date_key not in csv_dict:
          csv_dict[date_key] = []
        csv_dict[date_key].append(os.path.join(folder_path, file_name))

  # Merge files for each date and write to the output folder
  for date_key, files in csv_dict.items():
    with open(f"{output_folder_path}/merged_{date_key}.csv", 'w', newline='') as merged_file:
      writer = csv.writer(merged_file)
      header_written = False
      for file_path in files:
        with open(file_path, 'r') as csv_file:
          reader = csv.reader(csv_file)
          try:
            header = next(reader)
          except StopIteration:
            header = []
          if not header_written:
            # Write header only once for each date file
            if header:
              writer.writerow(header)
            else:
              default_header = ["timestamp", "domain", "dns_server", "record_type", "answers", "error_code", "error_reason"]
              writer.writerow(default_header)
            header_written = True
          for row in reader:
            writer.writerow(row)

if __name__ == "__main__":
  folder_paths = [
    "/Users/silverhand/Developer/SourceRepo/GFW-Research/src/Lib/Data-2024-11-12/China-Mobile/DNSPoisoning/Nov14",
    "/Users/silverhand/Developer/SourceRepo/GFW-Research/src/Lib/Data-2024-11-12/China-Mobile/DNSPoisoning/Nov15",
    "/Users/silverhand/Developer/SourceRepo/GFW-Research/src/Lib/Data-2024-11-12/China-Mobile/DNSPoisoning"
  ]
  output_folder_path = "/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile"
  merge_csv(folder_paths, output_folder_path)
