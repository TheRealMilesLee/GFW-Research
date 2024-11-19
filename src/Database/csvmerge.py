import csv
import os


def merge_csv(folder_paths: list, output_folder_path: str) -> None:
  """
  Merges CSV files from specified folders into a single CSV file per date.

  @param folder_paths: A list of paths to the folders containing the input CSV files.
  @param output_folder_path: The path to the folder where the merged CSV files will be saved.
  @return: None
  """
  csv_dict = {}

  # Define default header
  default_header = ["timestamp", "domain", "dns_server", "record_type", "answers", "error_code", "error_reason"]

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
    output_file_path = os.path.join(output_folder_path, f"merged_{date_key}.csv")
    with open(output_file_path, 'w', newline='') as merged_file:
      writer = csv.writer(merged_file)
      header_written = False
      for file_path in files:
        with open(file_path, 'r') as csv_file:
          reader = csv.reader(csv_file)
          try:
            header = next(reader)  # Read the header
            if not header_written:
              # Write header only once for each date file
              writer.writerow(header if header else default_header)
              header_written = True
          except StopIteration:
            # Handle empty files by adding a default header
            if not header_written:
              writer.writerow(default_header)
              header_written = True
            continue
          for row in reader:
            writer.writerow(row)

if __name__ == "__main__":
  # Step 1: Merge Nov14 and Nov15 files
  nov14_15_paths = [
    "/Users/silverhand/Developer/SourceRepo/GFW-Research/src/Lib/Data-2024-11-12/China-Mobile/DNSPoisoning/Nov14",
    "/Users/silverhand/Developer/SourceRepo/GFW-Research/src/Lib/Data-2024-11-12/China-Mobile/DNSPoisoning/Nov15"
  ]
  nov14_15_output_path = "/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile"
  merge_csv(nov14_15_paths, nov14_15_output_path)

  # Step 2: Merge files from Nov15 onwards
  post_nov15_paths = [
    "/Users/silverhand/Developer/SourceRepo/GFW-Research/src/Lib/Data-2024-11-12/China-Mobile/DNSPoisoning"
  ]
  post_nov15_output_path = "/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile"
  merge_csv(post_nov15_paths, post_nov15_output_path)
