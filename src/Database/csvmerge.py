import csv
import os


def merge_csv(folder_paths: list, output_folder_path: str) -> None:
  csv_dict = {}

  for folder_path in folder_paths:
    print(f"Checking folder: {folder_path}")  # Debugging folder being checked
    file_list = os.listdir(folder_path)
    for file_name in file_list:
      if file_name.endswith('.csv'):
        print(f"Found file: {file_name}")  # Debugging file being checked
        parts = file_name.split('_')
        print(f"Parts for file {file_name}: {parts}")  # Debugging parts

        # Handle two file formats
        if len(parts) == 7:  # Format: DNS_Checking_Result_YYYY_MM_DD_HH_MM.csv
          date_key = '_'.join(parts[3:6])  # Extract YYYY_MM_DD
        elif len(parts) == 6:  # Format: DNS_Checking_Result_YYYY_MM_DD.csv
          date_key = '_'.join(parts[3:6])  # Extract YYYY_MM_DD
        else:
          print(f"Skipping file with unexpected format: {file_name}")
          continue

        if date_key not in csv_dict:
          csv_dict[date_key] = []
        csv_dict[date_key].append(os.path.join(folder_path, file_name))

  # Merge CSV files
  for date_key, files in csv_dict.items():
    print(f"Merging files for date: {date_key}")
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

          # Add default header if none found
          if not header_written:
            if header:
              writer.writerow(header)
            else:
              default_header = [
                "timestamp", "domain", "dns_server",
                "record_type", "answers", "error_code", "error_reason"
              ]
              writer.writerow(default_header)
            header_written = True

          # Write remaining rows
          for row in reader:
            writer.writerow(row)

if __name__ == "__main__":
  if os.name == 'nt':
    folder_paths = [
      "E:\\Developer\\SourceRepo\\GFW-Research\\src\\Lib\\Data-2024-11-12\\China-Mobile\\DNSPoisoning"
    ]
    output_folder_path = "E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\Data-2024-11\\ChinaMobile"
  else:
    folder_paths = [
      "/Users/silverhand/Developer/SourceRepo/GFW-Research/src/Lib/Data-2024-11-12/China-Mobile/DNSPoisoning"
    ]
    output_folder_path = "/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile"
  merge_csv(folder_paths, output_folder_path)
