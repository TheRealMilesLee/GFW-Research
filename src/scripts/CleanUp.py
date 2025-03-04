from ..Database.DBOperations import Merged_db, MongoDBHandler, ADC_db
import csv
import os
from datetime import datetime, timedelta
import concurrent.futures

DNSPoisoning = MongoDBHandler(Merged_db["DNSPoisoning"])
merged_2024_Nov_DNS = MongoDBHandler(Merged_db["2024_Nov_DNS"])
merged_2025_Jan_DNS = MongoDBHandler(Merged_db["2025_DNS"])
adc_2025_Jan_DNS = MongoDBHandler(
    ADC_db["ChinaMobile-DNSPoisoning-2025-January"])
GFWLocation = MongoDBHandler(Merged_db["TraceRouteResult"])
merge_db_2024_Nov_GFWL = MongoDBHandler(Merged_db["2024_Nov_GFWL"])
adc_db_2025_GFWL = MongoDBHandler(
    ADC_db["ChinaMobile-GFWLocation-2025-January"])


def cleanDomains():
  """
  1. 从Lib/CompareGroup/DNSPoisoning/中读取所有的CSV文件
  2. 将CSV文件中的数据导入到数据库中
  3. 读取/home/silverhand/Developer/SourceRepo/GFW-Research/src/Import/domains_list.csv
  4. 对于每一个domain, 在数据库中查找, 如果所有的结果的answer栏都为空, 则print出这个domain
  """
  # 1. 从Lib/CompareGroup/DNSPoisoning/中读取所有的CSV文件
  folder_path = '/home/lhengyi/Developer/GFW-Research/Lib/CompareGroup/DNSPoisoning'
  all_results = []
  for root, dirs, files in os.walk(folder_path):
    for file in files:
      if file.endswith('.csv'):
        file_path = os.path.join(root, file)
        with open(file_path, 'r') as file:
          reader = csv.DictReader(file)
          for row in reader:
            all_results.append(row)

  print(f"Total {len(all_results)} records")
  # 2. 将CSV文件中的数据导入到数据库中
  db = MongoDBHandler(Merged_db['CompareGroup'])
  print("Clearing the database")
  db.delete_many({})
  print("Inserting the records to the database")
  db.insert_many(all_results)
  # 3. 读取/home/silverhand/Developer/SourceRepo/GFW-Research/src/Import/domains_list.csv
  file_path = os.path.join(os.path.dirname(__file__),
                           '../Import/domains_list.csv')
  with open(file_path, 'r') as file:
    reader = csv.reader(file)
    domains = [row[0].strip() for row in reader]

  def check_domain(domain):
    results = db.find({"domain": domain})
    if results and all([
        not result['answers'] or result['answers'] == '[]'
        for result in results
    ]):
      with open("InvalidDomains.txt", "w") as file:
        file.write(f"{domain}\n")
      print(f"{domain} is invalid")

  # 4. 对于每一个domain, 在数据库中查找, 如果所有的结果的answer栏都为空或者空array, 则print出这个domain
  with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(check_domain, domains)

  # 5. 根据拿到的InvalidDomains.txt, 从ADC_db, BDC_db, Merge_db中删除这些domain
  with open("InvalidDomains.txt", "r") as file:
    invalid_domains = [line.strip() for line in file]
  print(f"Total {len(invalid_domains)} invalid domains")
  for domain in invalid_domains:
    DNSPoisoning.delete_many({"domain": domain})
    merged_2024_Nov_DNS.delete_many({"domain": domain})
    merged_2025_Jan_DNS.delete_many({"domain": domain})
    adc_2025_Jan_DNS.delete_many({"domain": domain})
    GFWLocation.delete_many({"domain": domain})
    merge_db_2024_Nov_GFWL.delete_many({"domain": domain})
    adc_db_2025_GFWL.delete_many({"domain": domain})


if __name__ == "__main__":
  cleanDomains()
