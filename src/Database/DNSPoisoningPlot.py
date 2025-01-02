import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
from DBOperations import CompareGroup_db, Merged_db, MongoDBHandler
import concurrent.futures
from tqdm import tqdm
import re
from collections import Counter
import subprocess
import tldextract
import pycountry
import csv

# Merged_db constants
DNSPoisoning = MongoDBHandler(Merged_db["DNSPoisoning"])

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

def DNSPoisoning_ErrorCode_Distribute():
  """
  Plot the distribution of error codes in the DNS poisoning data.
  """
  # Count the number of occurrences of each error code
  error_code_count = {}
  for category in categories:
    error_code_count[category] = DNSPoisoning.count_documents({'error_code': category})

  # Plot the distribution of error codes, with the number on each bar
  plt.figure(figsize=(12, 6))
  plt.bar(error_code_count.keys(), error_code_count.values())
  for category, count in error_code_count.items():
    plt.text(category, count, str(count), ha='center', va='bottom')
  plt.xlabel('Error Code')
  plt.ylabel('Number of Occurrences')
  plt.title('Distribution of Error Codes in DNS Poisoning Data')
  plt.savefig('DNSPoisoning_ErrorCode_Distribute.png')
  plt.close()

def process_domain(data):
  domain = data['domain']
  error_code_count = {}
  for category in categories:
    error_code_count[category] = DNSPoisoning.count_documents({'domain': domain, 'error_code': category})

  total_count = sum(error_code_count.values())
  folder_name = 'DNSPoisoning' if total_count > 0 else 'DNSPoisoning_Empty'

  plt.figure(figsize=(12, 6))
  plt.bar(error_code_count.keys(), error_code_count.values())
  for category, count in error_code_count.items():
    plt.text(category, count, str(count), ha='center', va='bottom')
  plt.xlabel('Error Code')
  plt.ylabel('Number of Occurrences')
  plt.title(f'Distribution of Error Codes in DNS Poisoning Data of {domain}')
  plt.savefig(f'{folder_name}/{domain}.png')
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


def distribution_NXDomain():
  docs = DNSPoisoning.find({"error_code": "NXDOMAIN"}, {"error_reason": 1})
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

  plt.figure(figsize=(12, 6))
  wedges, texts, autotexts = plt.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  plt.title('NXDOMAIN Error Reason Distribution by Location')
  plt.legend(wedges, location_counts.keys(), title="Locations", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
  plt.savefig('NXDOMAIN_Distribution_by_Location.png', bbox_inches='tight')
  plt.close()

def distribution_NXDomain_exclude_yandex():
  docs = DNSPoisoning.find({"error_code": "NXDOMAIN"}, {"error_reason": 1})
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

  plt.figure(figsize=(12, 6))
  wedges, texts, autotexts = plt.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  plt.title('NXDOMAIN Error Reason Distribution by Location (Yandex Excluded)')
  plt.legend(wedges, location_counts.keys(), title="Locations", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
  plt.savefig('NXDOMAIN_Distribution_by_Location_Yandex_excluded.png', bbox_inches='tight')
  plt.close()


if __name__ == '__main__':
  DNSPoisoning_ErrorCode_Distribute()
  # dataFromMerged = DNSPoisoning.getAllDocuments()
  # with concurrent.futures.ProcessPoolExecutor() as executor:
  #   list(tqdm(executor.map(process_domain, dataFromMerged), total=len(dataFromMerged), desc='Processing domains'))
  distribution_NXDomain()
  distribution_NXDomain_exclude_yandex()
