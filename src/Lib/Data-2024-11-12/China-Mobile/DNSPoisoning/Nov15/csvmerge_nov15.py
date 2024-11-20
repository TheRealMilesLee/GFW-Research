import csv
import os


def merge_csv(folder_paths: list, output_file_path: str) -> None:
  csv_files = []

  for folder_path in folder_paths:
    print(f"Checking folder: {folder_path}")  # Debugging folder being checked
    file_list = os.listdir(folder_path)
    for file_name in file_list:
      if file_name.endswith('.csv'):
        print(f"Found file: {file_name}")  # Debugging file being checked
        csv_files.append(os.path.join(folder_path, file_name))

  # Merge CSV files into a single file
  with open(output_file_path, 'w', newline='') as merged_file:
    writer = csv.writer(merged_file)
    header_written = False
    for file_path in csv_files:
      with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        try:
          header = next(reader)
        except StopIteration:
          header = []

        # Write header only once
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
      "E:\\Developer\\SourceRepo\\GFW-Research\\src\\Lib\\Data-2024-11-12\\China-Mobile\\DNSPoisoning\\Nov15"
    ]
    output_file_path = "E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\Data-2024-11\\ChinaMobile\\merged_2024_11_15.csv"
  else:
    folder_paths = [
      "/Users/silverhand/Developer/SourceRepo/GFW-Research/src/Lib/Data-2024-11-12/China-Mobile/DNSPoisoning/Nov14"
    ]
    output_file_path = "/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile/merged_2024_11_15.csv"
  merge_csv(folder_paths, output_file_path)
