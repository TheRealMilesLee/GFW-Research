import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
from ..DBOperations import Merged_db, MongoDBHandler
import re
from collections import Counter
import csv

# Merged_db constants
DNSPoisoning = MongoDBHandler(Merged_db["DNSPoisoning"])
GFWLocation = MongoDBHandler(Merged_db["GFWLocation"])
merged_2024_Nov_DNS = MongoDBHandler(Merged_db["2024_Nov_DNS"])
merged_2024_Nov_GFWL = MongoDBHandler(Merged_db["2024_Nov_GFWL"])
merged_2025_Jan_DNS = MongoDBHandler(Merged_db["2025_DNS"])
merged_2025_Jan_GFWL = MongoDBHandler(Merged_db["2025_GFWL"])

categories = DNSPoisoning.distinct('error_code')

# 读取 CSV 文件并创建 IP 到 Provider 的映射
ip_to_provider = {}
with open('E:\\Developer\\SourceRepo\\GFW-Research\\src\\Import\\dns_servers.csv', 'r') as csvfile:
  reader = csv.DictReader(csvfile)
  for row in reader:
    if row['IPV4']:
      ip_to_provider[row['IPV4']] = row['Provider']
    if row['IPV6']:
      ip_to_provider[row['IPV6']] = row['Provider']

def DNSPoisoning_ErrorCode_Distribute(destination_db, output_folder):
  """
  Plot the distribution of error codes in the DNS poisoning data.
  """
  # Count the number of occurrences of each error code
  error_code_count = {}
  for category in categories:
    error_code_count[category] = destination_db.count_documents({'error_code': category})

  # Plot the distribution of error codes, with the number on each bar
  plt.figure(figsize=(12, 6))
  plt.bar(error_code_count.keys(), error_code_count.values())
  for category, count in error_code_count.items():
    plt.text(category, count, str(count), ha='center', va='bottom')
  plt.xlabel('Error Code')
  plt.ylabel('Number of Occurrences')
  plt.title('Distribution of Error Codes in DNS Poisoning Data')
  plt.savefig(f'{output_folder}/DNSPoisoning_ErrorCode_Distribute.png')
  plt.close()

def get_server_location(server):
  if server in ip_to_provider:
    return ip_to_provider[server]
  else:
    regex = re.compile(r'^(.*?)(?: Timeout| Non-existent | No)')
    ip_match = regex.match(server)
    if ip_match:
      return ip_to_provider.get(ip_match.group(1), 'Unknown')
    else:
      print(f'Unknown server: {server}')
      return 'Unknown'

def distribution_NXDomain(destination_db, output_folder):
  docs = destination_db.find({"error_code": "NXDOMAIN"}, {"error_reason": 1})
  location_counts = Counter()
  for doc in docs:
    error_reason = doc.get('error_reason', '')
    if isinstance(error_reason, list):
      error_reason = ' '.join(error_reason)
    match = re.search(r'on server:\s*(.*)$', error_reason)
    if match:
      server = match.group(1).strip()
      location = get_server_location(server)
      location_counts[location] += 1

  plt.figure(figsize=(15, 6))
  wedges, texts, autotexts = plt.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  plt.title('NXDOMAIN Error Reason Distribution by Location')
  plt.savefig(f'{output_folder}/NXDOMAIN_Distribution_by_Location.png', bbox_inches='tight')
  plt.close()

def distribution_NXDomain_exclude_yandex(destination_db, output_folder):
  docs = destination_db.find({"error_code": "NXDOMAIN"}, {"error_reason": 1})
  location_counts = Counter()
  for doc in docs:
    error_reason = doc.get('error_reason', '')
    if isinstance(error_reason, list):
      error_reason = ' '.join(error_reason)
    match = re.search(r'on server:\s*(.*)$', error_reason)
    if match:
      server = match.group(1).strip()
      location = get_server_location(server)
      if location != 'Yandex DNS' and location != 'Yandex DNS (Additional)':
        location_counts[location] += 1

  plt.figure(figsize=(15, 6))
  wedges, texts, autotexts = plt.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  plt.title('NXDOMAIN Error Reason Distribution by Location (Yandex Excluded)')
  plt.savefig(f'{output_folder}/NXDOMAIN_Distribution_by_Location_Yandex_excluded.png', bbox_inches='tight')
  plt.close()

def distribution_NoAnswer(destination_db, output_folder):
  docs = destination_db.find({"error_code": "NoAnswer"}, {"error_reason": 1})
  location_counts = Counter()
  for doc in docs:
    error_reason = doc.get('error_reason', '')
    if isinstance(error_reason, list):
      error_reason = ' '.join(error_reason)
    match = re.search(r'on server:\s*(.*)$', error_reason)
    if match:
      server = match.group(1).strip()
      location = get_server_location(server)
      location_counts[location] += 1

  plt.figure(figsize=(15, 6))
  wedges, texts, autotexts = plt.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  plt.title('NoAnswer Error Reason Distribution by Location')
  plt.savefig(f'{output_folder}/NoAnswer_Distribution_by_Location.png', bbox_inches='tight')
  plt.close()

def distribution_NoNameservers(destination_db, output_folder):
  docs = destination_db.find({"error_code": "NoNameservers"}, {"error_reason": 1})
  location_counts = Counter()
  for doc in docs:
    error_reason = doc.get('error_reason', '')
    if isinstance(error_reason, list):
      error_reason = ' '.join(error_reason)
    match = re.search(r'on server:\s*(.*)$', error_reason)
    if match:
      server = match.group(1).strip()
      location = get_server_location(server)
      location_counts[location] += 1

  plt.figure(figsize=(15, 6))
  wedges, texts, autotexts = plt.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  plt.title('NoNameservers Error Reason Distribution by Location')
  plt.savefig(f'{output_folder}/NoNameservers_Distribution_by_Location.png', bbox_inches='tight')
  plt.close()

def distribution_Timeout(destination_db, output_folder):
  docs = destination_db.find({"error_code": "Timeout"}, {"error_reason": 1})
  location_counts = Counter()
  for doc in docs:
    error_reason = doc.get('error_reason', '')
    if isinstance(error_reason, list):
      error_reason = ' '.join(error_reason)
    match = re.search(r'on server:\s*(.*)$', error_reason)
    if match:
      server = match.group(1).strip()
      location = get_server_location(server)
      location_counts[location] += 1

  plt.figure(figsize=(15, 6))
  wedges, texts, autotexts = plt.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  plt.title('Timeout Error Reason Distribution by Location')
  plt.savefig(f'{output_folder}/Timeout_Distribution_by_Location.png', bbox_inches='tight')
  plt.close()

# 查找数据库中包含 rst_detected 字段的文档，统计 rst_detected 字段的值的分布
def distribution_GFWL_rst_detected(destination_db, output_folder):
  total_docs = destination_db.count_documents({})
  docs_with_rst_detected = destination_db.count_documents({"rst_detected": { "$elemMatch": { "$exists": True } }})
  rst_detected_ratio = docs_with_rst_detected / total_docs * 100

  location_counts = Counter()
  docs = destination_db.find({"rst_detected": { "$elemMatch": { "$exists": True } }})
  for doc in docs:
    for rst_detected in doc['rst_detected']:
      location_counts[rst_detected] += 1

  plt.figure(figsize=(15, 6))
  wedges, texts, autotexts = plt.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  plt.title(f'RST Detected Distribution (Ratio: {rst_detected_ratio:.2f}%)')
  plt.savefig(f'{output_folder}/RST_Detected_Distribution.png', bbox_inches='tight')
  plt.close()

# 查找数据库中包含redirection_detected字段的文档，统计redirection_detected字段的值的分布
def distribution_GFWL_redirection_detected(destination_db, output_folder):
  total_docs = destination_db.count_documents({})
  docs_with_redirection_detected = destination_db.count_documents({"redirection_detected": { "$elemMatch": { "$exists": True } }})
  redirection_detected_ratio = docs_with_redirection_detected / total_docs * 100

  location_counts = Counter()
  docs = destination_db.find({"redirection_detected": { "$elemMatch": { "$exists": True } }})
  for doc in docs:
    for redirection_detected in doc['redirection_detected']:
      location_counts[redirection_detected] += 1

  plt.figure(figsize=(15, 6))
  wedges, texts, autotexts = plt.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  plt.title(f'Redirection Detected Distribution (Ratio: {redirection_detected_ratio:.2f}%)')
  plt.savefig(f'{output_folder}/Redirection_Detected_Distribution.png', bbox_inches='tight')
  plt.close()

# 查找数据库中Error字段的文档，统计Error字段的值的分布
def distribution_GFWL_Error(destination_db, output_folder):
  total_docs = destination_db.count_documents({})
  docs_with_error = destination_db.count_documents({"Error": { "$elemMatch": { "$exists": True } }})
  error_ratio = docs_with_error / total_docs * 100

  location_counts = Counter()
  docs = destination_db.find({"Error": { "$elemMatch": { "$exists": True } }})
  for doc in docs:
    for error in doc['Error']:
      location_counts[error] += 1

  plt.figure(figsize=(15, 6))
  wedges, texts, autotexts = plt.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  plt.title(f'Error Distribution (Ratio: {error_ratio:.2f}%)')
  plt.savefig(f'{output_folder}/Error_Distribution.png', bbox_inches='tight')
  plt.close()


if __name__ == '__main__':
  DNSPoisoning_ErrorCode_Distribute(DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_NXDomain(DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_NXDomain_exclude_yandex(DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_NoAnswer(DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_NoNameservers(DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_Timeout(DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')

  DNSPoisoning_ErrorCode_Distribute(merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_NXDomain(merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_NXDomain_exclude_yandex(merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_NoAnswer(merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_NoNameservers(merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_Timeout(merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_GFWL_rst_detected(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_GFWL_redirection_detected(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_GFWL_Error(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')

  DNSPoisoning_ErrorCode_Distribute(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_NXDomain(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_NXDomain_exclude_yandex(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_NoAnswer(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_NoNameservers(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_Timeout(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_GFWL_rst_detected(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_GFWL_redirection_detected(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_GFWL_Error(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
