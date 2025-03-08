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


def read_csv_files(folder_path):
  all_results = []
  for root, dirs, files in os.walk(folder_path):
    for file in files:
      if file.endswith('.csv'):
        file_path = os.path.join(root, file)
        with open(file_path, 'r') as file:
          reader = csv.DictReader(file)
          for row in reader:
            # Ensure columns correspond to timestamp, domain, dns_server, record_type, ips, error_code, error_reason
            if set(row.keys()) == {
                "timestamp", "domain", "dns_server", "record_type", "ips",
                "error_code", "error_reason"
            }:
              # Convert ips from string to array
              row["ips"] = row["ips"].strip('[]').split(', ')
              all_results.append(row)
            else:
              print(f"Skipping file {file_path} due to incorrect columns")
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


def check_domain(compare_db, target_db, domain):
  compare_results = compare_db.find({"domain": domain})
  target_results = target_db.find({"domain": domain})
  # 如果对比库与目标库的IPS都为空则视为无效域名
  if compare_results and all([not r['ips'] or r['ips'] == '[]' for r in compare_results]) \
     and target_results and all([not r['ips'] or r['ips'] == '[]' for r in target_results]):
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
  folder_path = '/home/lhengyi/Developer/GFW-Research/Lib/CompareGroup/DNSPoisoning'
  all_results = read_csv_files(folder_path)
  print(f"Total {len(all_results)} records")

  db = MongoDBHandler(Merged_db['CompareGroup'])
  import_to_db(db, all_results)

  domains_file_path = os.path.join(os.path.dirname(__file__),
                                   '../Import/domains_list.csv')
  domains = read_domains(domains_file_path)

  with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
    for domain in domains:
      executor.submit(check_domain, db, DNSPoisoning, domain)

  if os.path.exists("InvalidDomains.txt"):
    with open("InvalidDomains.txt", "r") as file:
      invalid_domains = [line.strip() for line in file]
    print(f"Total {len(invalid_domains)} invalid domains")
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
      for domain in invalid_domains:
        delete_domain(domain)

  with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
    executor.submit(cleanNoAnswer, merged_2024_Nov_DNS)
    executor.submit(cleanNoAnswer, merged_2025_Jan_DNS)
    executor.submit(cleanNoAnswer, adc_2025_Jan_DNS)
    executor.submit(cleanNoAnswer, DNSPoisoning)


def cleanNoAnswer(db):
  """
    1. 查询 error_code 包含 NoAnswer 的记录
    2. 检查同一 domain 下 record_type 是否同时有 A 和 AAAA 记录含 NoAnswer
    3. 如果同时出现，则保留；否则从 error_code 中删除 NoAnswer这个entry
    """
  print(f"Cleaning NoAnswer for {db.collection.name}")
  total_docs = db.count_documents({"error_code": "NoAnswer"})
  processed = 0
  batch_size = 20000
  cursor = db.find({"error_code": "NoAnswer"}).batch_size(batch_size)

  for result in cursor:
    domain = result["domain"]

    # 查询该 domain 是否有 A 和 AAAA 类型的 NoAnswer 记录
    has_ipv4 = db.find_one({
        "domain": domain,
        "record_type": "A",
        "error_code": "NoAnswer"
    }) is not None

    has_ipv6 = db.find_one({
        "domain": domain,
        "record_type": "AAAA",
        "error_code": "NoAnswer"
    }) is not None

    # 如果 A 和 AAAA 记录都含有 NoAnswer，则保留
    if has_ipv4 and has_ipv6:
      continue

    # 仅删除 NoAnswer的entry
    db.update_many(
        {
            "domain": domain,
            "record_type": {
                "$in": ["A", "AAAA"]
            },  # 仅作用于 A 和 AAAA 记录
            "error_code": "NoAnswer"
        },
        {"$pull": {
            "error_code": "NoAnswer"
        }},
        upsert=False)
    processed += 1
    if processed % 10000 == 0:
      print(f"已处理 {processed}/{total_docs} 条文档")
  cursor.close()
  print(f"Cleaned NoAnswer for {db.collection.name}")


if __name__ == "__main__":
  cleanDomains()
