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

  DNSPoisoning_ErrorCode_Distribute(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_NXDomain(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_NXDomain_exclude_yandex(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_NoAnswer(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_NoNameservers(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_Timeout(merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
