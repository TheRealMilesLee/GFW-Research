import csv
import os


def merge_csv():
  folder_path = "/Users/silverhand/Developer/SourceRepo/GFW-Research/src/Lib/Data-2024-11-12/China-Mobile/DNSPoisoning"
  output_folder_path = "/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile"
  file_list = os.listdir(folder_path)
  csv_dict = {}

  for file_name in file_list:
    if file_name.endswith('.csv'):
      date_str = file_name.split('_')[3:6]
      date_key = '_'.join(date_str[:3])
      if date_key not in csv_dict:
        csv_dict[date_key] = []
      csv_dict[date_key].append(file_name)

  for date_key, files in csv_dict.items():
    with open(f"{output_folder_path}/merged_{date_key}.csv", 'w', newline='') as merged_file:
      writer = csv.writer(merged_file)
      header_written = False
      for file_name in files:
        with open(f"{folder_path}/{file_name}", 'r') as csv_file:
          reader = csv.reader(csv_file)
          header = next(reader)
          if not header_written:
            writer.writerow(header)
            header_written = True
          for row in reader:
            writer.writerow(row)

if __name__ == "__main__":
  merge_csv()
