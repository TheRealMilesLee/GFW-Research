from ..Database.DBOperations import Merged_db, MongoDBHandler, ADC_db
import csv
import os
import concurrent.futures
import multiprocessing

DNSPoisoning = MongoDBHandler(Merged_db["DNSPoisoning"])
merged_2024_Nov_DNS = MongoDBHandler(Merged_db["2024_Nov_DNS"])
merged_2025_Jan_DNS = MongoDBHandler(Merged_db["2025_DNS"])
adc_2025_Jan_DNS = MongoDBHandler(
    ADC_db["ChinaMobile-DNSPoisoning-2025-January"])
GFWLocation = MongoDBHandler(Merged_db["TraceRouteResult"])
merge_db_2024_Nov_GFWL = MongoDBHandler(Merged_db["2024_Nov_GFWL"])
adc_db_2025_GFWL = MongoDBHandler(
    ADC_db["ChinaMobile-GFWLocation-2025-January"])
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 64)  # Dynamically set workers


def read_csv_files(folder_path):
  all_results = []
  for root, dirs, files in os.walk(folder_path):
    for file in files:
      if file.endswith('.csv'):
        file_path = os.path.join(root, file)
        with open(file_path, 'r') as file:
          reader = csv.DictReader(file)
          for row in reader:
            all_results.append(row)
  return all_results


def import_to_db(db, data):
  print("Clearing the database")
  db.delete_many({})
  print("Inserting the records to the database")
  db.insert_many(data)


def read_domains(file_path):
  with open(file_path, 'r') as file:
    reader = csv.reader(file)
    domains = [row[0].strip() for row in reader]
  return domains


def check_domain(db, domain):
  results = db.find({"domain": domain})
  if results and all([
      not result['answers'] or result['answers'] == '[]' for result in results
  ]):
    with open("InvalidDomains.txt", "a") as file:
      file.write(f"{domain}\n")
    print(f"{domain} is invalid")


def delete_domain(domain):
  DNSPoisoning.delete_many({"domain": domain})
  merged_2024_Nov_DNS.delete_many({"domain": domain})
  merged_2025_Jan_DNS.delete_many({"domain": domain})
  adc_2025_Jan_DNS.delete_many({"domain": domain})
  GFWLocation.delete_many({"domain": domain})
  merge_db_2024_Nov_GFWL.delete_many({"domain": domain})
  adc_db_2025_GFWL.delete_many({"domain": domain})


def cleanDomains():
  folder_path = '/home/silverhand/Developer/SourceRepo/GFW-Research/Lib/CompareGroup/DNSPoisoning'
  all_results = read_csv_files(folder_path)
  print(f"Total {len(all_results)} records")

  db = MongoDBHandler(Merged_db['CompareGroup'])
  import_to_db(db, all_results)

  domains_file_path = os.path.join(os.path.dirname(__file__),
                                   '../Import/domains_list.csv')
  domains = read_domains(domains_file_path)

  with concurrent.futures.ThreadPoolExecutor(
      max_workers=MAX_WORKERS) as executor:
    for domain in domains:
      executor.submit(check_domain, db, domain)

  with open("InvalidDomains.txt", "r") as file:
    invalid_domains = [line.strip() for line in file]
  print(f"Total {len(invalid_domains)} invalid domains")

  with concurrent.futures.ThreadPoolExecutor(
      max_workers=MAX_WORKERS) as executor:
    for domain in invalid_domains:
      executor.submit(delete_domain, domain)


if __name__ == "__main__":
  cleanDomains()
