import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
from ..DBOperations import Merged_db, MongoDBHandler
import re
from collections import Counter
import csv
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import matplotlib.dates as mdates
# Merged_db constants
DNSPoisoning = MongoDBHandler(Merged_db["DNSPoisoning"])
merged_2024_Nov_DNS = MongoDBHandler(Merged_db["2024_Nov_DNS"])
merged_2025_Jan_DNS = MongoDBHandler(Merged_db["2025_DNS"])

categories = DNSPoisoning.distinct('error_code')

# 添加提供商到区域的映射
provider_region_map = {
  "Google": "U.S. DNS Provider",
  "Google Alternative": "U.S. DNS Provider",
  "Cloudflare": "U.S. DNS Provider",
  "Cloudflare Alternative": "U.S. DNS Provider",
  "GCore": "Luxembourg DNS Provider",
  "GCore Alternative": "Luxembourg DNS Provider",
  "Yandex DNS": "Russia DNS Provider",
  "Yandex DNS Alternative": "Russia DNS Provider",
  "Yandex DNS (Additional)": "Russia DNS Provider",
  "Quad9": "Zürich DNS Provider",
  "Quad9 Alternative": "Zürich DNS Provider",
  "OpenDNS": "U.S. DNS Provider",
  "OpenDNS Alternative": "U.S. DNS Provider",
  "OpenDNS Additional": "U.S. DNS Provider",
  "AdGuard DNS": "Cyprus DNS Provider",
  "AdGuard DNS Alternative": "Cyprus DNS Provider",
  "114DNS": "China DNS Provider",
  "114DNS Alternative": "China DNS Provider",
  "AliDNS": "China DNS Provider",
  "AliDNS Alternative": "China DNS Provider",
  "DNSPod": "China DNS Provider",
  "Baidu DNS": "China DNS Provider",
  "China Telecom": "China DNS Provider",
  "China Unicom": "China DNS Provider",
  "CUCC DNS": "China DNS Provider",
  "China Mobile": "China DNS Provider",
  "OneDNS": "China DNS Provider",
  "Tencent DNS": "China DNS Provider",
  "Baidu DNS Alternative": "China DNS Provider",
  "Tencent DNS Alternative": "China DNS Provider"
}

# 读取 CSV 文件并创建 IP 到 Provider 的映射
ip_to_provider = {}
ip_to_region = {}
with open('E:\\Developer\\SourceRepo\\GFW-Research\\src\\Import\\dns_servers.csv', 'r') as csvfile:
  reader = csv.DictReader(csvfile)
  for row in reader:
    provider = row['Provider']
    region = provider_region_map.get(provider, 'Other DNS Provider')
    if row['IPV4']:
      ip_to_provider[row['IPV4']] = provider
      ip_to_region[row['IPV4']] = region
    if row['IPV6']:
      ip_to_provider[row['IPV6']] = provider
      ip_to_region[row['IPV6']] = region

def DNSPoisoning_ErrorCode_Distribute(destination_db, output_folder):
  """
  Plot the distribution of error codes for each DNS server in the DNS poisoning data.
  """
  dns_servers = list(ip_to_provider.keys())
  for server in dns_servers:
    provider = ip_to_provider.get(server, 'Unknown Provider')  # 获取提供商名称
    # 统计当前服务器每个错误代码的出现次数
    error_code_count = Counter()
    docs = destination_db.find({'dns_server': server})
    for doc in docs:
      error_code = doc.get('error_code')
      if error_code:
        error_code_str = str(error_code) if isinstance(error_code, list) else error_code
        error_code_count[error_code_str] += 1

    if not error_code_count:
      print(f'No error codes found for DNS server: {server}')
      continue  # 跳过没有错误代码的服务器

    # 绘制错误代码分布柱状图
    fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)  # 使用基于 Figure 的接口
    error_codes = list(error_code_count.keys())
    counts = list(error_code_count.values())
    ax.bar(error_codes, counts, color='skyblue')
    for i, count in enumerate(counts):
      ax.text(i, count, str(count), ha='center', va='bottom')
    ax.set_xlabel('Error Code')
    ax.set_ylabel('Number of Occurrences')
    ax.set_title(f'Error Code Distribution for DNS Server {server} ({provider})')
    plt.setp(ax.get_xticklabels(), rotation=45)
    sanitized_server = server.replace(':', '_').replace('/', '_')
    fig.savefig(f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_{sanitized_server}_{provider}.png')
    plt.close(fig)  # 确保图形被关闭

def DNSPoisoning_ErrorCode_Distribute_ProviderRegion(destination_db, output_folder):
  """
  按区域绘制错误代码分布，以对比中国区与其他区域的DNS服务商在错误代码上的差异。
  """
  print('Plotting error code distribution by provider region...')
  try:
    region_to_servers = {}
    for server, region in ip_to_region.items():
      region_to_servers.setdefault(region, []).append(server)

    for region, servers in region_to_servers.items():
      error_code_count = Counter()
      for server in servers:
        provider = ip_to_provider.get(server, 'Unknown Provider')
        docs = destination_db.find({'dns_server': server})
        for doc in docs:
          error_code = doc.get('error_code')
          if error_code:
            error_code_str = str(error_code) if isinstance(error_code, list) else error_code
            error_code_count[error_code_str] += 1

      if not error_code_count:
        print(f'No error codes found for region: {region}')
        continue

      fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
      error_codes = list(error_code_count.keys())
      counts = list(error_code_count.values())
      ax.bar(error_codes, counts, color='skyblue')
      for i, count in enumerate(counts):
        ax.text(i, count, str(count), ha='center', va='bottom')
      ax.set_xlabel('Error Code')
      ax.set_ylabel('Number of Occurrences')
      ax.set_title(f'Error Code Distribution for {region}')
      plt.setp(ax.get_xticklabels(), rotation=45)
      sanitized_region = region.replace(':', '_').replace('/', '_')
      fig.savefig(f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_{sanitized_region}.png')
      plt.close(fig)
  except Exception as e:
    print(f'Error plotting error code distribution by provider region: {e}')

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

  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)  # 使用基于 Figure 的接口
  wedges, texts, autotexts = ax.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  ax.set_title('NXDOMAIN Error Reason Distribution by Location')
  fig.savefig(f'{output_folder}/NXDOMAIN_Distribution_by_Location.png', bbox_inches='tight')
  plt.close(fig)  # 确保图形被关闭

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

  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)  # 使用基于 Figure 的接口
  wedges, texts, autotexts = ax.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  ax.set_title('NXDOMAIN Error Reason Distribution by Location (Yandex Excluded)')
  fig.savefig(f'{output_folder}/NXDOMAIN_Distribution_by_Location_Yandex_excluded.png', bbox_inches='tight')
  plt.close(fig)  # 确保图形被关闭

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

  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)  # 使用基于 Figure 的接口
  wedges, texts, autotexts = ax.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  ax.set_title('NoAnswer Error Reason Distribution by Location')
  fig.savefig(f'{output_folder}/NoAnswer_Distribution_by_Location.png', bbox_inches='tight')
  plt.close(fig)  # 确保图形被关闭

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

  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)  # 使用基于 Figure 的接口
  wedges, texts, autotexts = ax.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  ax.set_title('NoNameservers Error Reason Distribution by Location')
  fig.savefig(f'{output_folder}/NoNameservers_Distribution_by_Location.png', bbox_inches='tight')
  plt.close(fig)  # 确保图形被关闭

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

  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)  # 使用基于 Figure 的接口
  wedges, texts, autotexts = ax.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  ax.set_title('Timeout Error Reason Distribution by Location')
  fig.savefig(f'{output_folder}/Timeout_Distribution_by_Location.png', bbox_inches='tight')
  plt.close(fig)  # 确保图形被关闭

def access_timely_distribution(destination_db, output_folder):
    """
    绘制能否访问的时间趋势
    """
    # 查询所有文档，提取域名和时间戳
    docs = destination_db.find({}, {"domain": 1, "timestamp": 1, "error_code": 1})
    access_data = {}

    for doc in docs:
        timestamp = doc.get('timestamp')
        domain = doc.get('domain')
        error_code = doc.get('error_code')
        if not timestamp or not domain:
            continue
        date = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        if date not in access_data:
            access_data[date] = {'accessible': set(), 'inaccessible': set()}
        if error_code:
            access_data[date]['inaccessible'].add(domain)
        else:
            access_data[date]['accessible'].add(domain)

    # 聚合数据
    dates = sorted(access_data.keys())
    accessible_counts = [len(access_data[date]['accessible']) for date in dates]
    inaccessible_counts = [len(access_data[date]['inaccessible']) for date in dates]

    # 绘制趋势图
    fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
    ax.plot(dates, accessible_counts, label='可访问', color='green')
    ax.plot(dates, inaccessible_counts, label='不可访问', color='red')
    ax.set_xlabel('时间戳')
    ax.set_ylabel('域名数量')
    ax.set_title('可访问与不可访问域名数量的时间趋势')
    ax.legend()

    # 格式化X轴为日期
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())

    plt.setp(ax.get_xticklabels(), rotation=45)
    fig.savefig(f'{output_folder}/access_timely_distribution.png')
    plt.close(fig)

def access_inaccess_distribution():
  """
  绘制可访问域名、不可访问域名和有时可访问域名的分布
  """
  # 查询所有文档，提取域名和时间戳
  docs = DNSPoisoning.find({}, {"domain": 1, "timestamp": 1, "error_code": 1, "ips": 1})
  access_data = {}

  for doc in docs:
    timestamp = doc.get('timestamp')
    domain = doc.get('domain')
    error_code = doc.get('error_code')
    result = doc.get('ips')
    if not timestamp or not domain:
      continue
    date = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    if date not in access_data:
      access_data[date] = {'accessible': set(), 'inaccessible': set(), 'sometimes': set()}
    if error_code and not result:
      access_data[date]['inaccessible'].add(domain)
    elif error_code and result:
      access_data[date]['sometimes'].add(domain)
    else:
      access_data[date]['accessible'].add(domain)

  # 聚合数据
  dates = sorted(access_data.keys())
  accessible_counts = [len(access_data[date]['accessible']) for date in dates]
  inaccessible_counts = [len(access_data[date]['inaccessible']) for date in dates]
  sometimes_counts = [len(access_data[date]['sometimes']) for date in dates]

  # 绘制趋势图
  fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
  ax.plot(dates, accessible_counts, label='可访问', color='green')
  ax.plot(dates, inaccessible_counts, label='不可访问', color='red')
  ax.plot(dates, sometimes_counts, label='有时可访问', color='orange')
  ax.set_xlabel('时间戳')
  ax.set_ylabel('域名数量')
  ax.set_title('可访问、不可访问和有时可访问域名数量的时间趋势')
  ax.legend()

  # 格式化X轴为日期
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
  ax.xaxis.set_major_locator(mdates.AutoDateLocator())

  plt.setp(ax.get_xticklabels(), rotation=45)
  fig.savefig('access_inaccess_distribution.png')
  plt.close(fig)

if __name__ == '__main__':
  def ensure_folder_exists(folder_path):
    if not os.path.exists(folder_path):
      os.makedirs(folder_path)

  folders = [
  'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9\\DNS_SERVER_DIST',
  'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9',
  'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11\\DNS_SERVER_DIST',
  'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11',
  'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1\\DNS_SERVER_DIST',
  'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'
  ]

  for folder in folders:
    ensure_folder_exists(folder)

  with ThreadPoolExecutor() as executor:
    tasks = [
      executor.submit(DNSPoisoning_ErrorCode_Distribute, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9\\DNS_SERVER_DIST'),
      executor.submit(DNSPoisoning_ErrorCode_Distribute_ProviderRegion, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9\\DNS_SERVER_DIST'),
      executor.submit(distribution_NXDomain, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),
      executor.submit(distribution_NXDomain_exclude_yandex, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),
      executor.submit(distribution_NoAnswer, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),
      executor.submit(distribution_NoNameservers, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),
      executor.submit(distribution_Timeout, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),
      executor.submit(access_timely_distribution, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),

      executor.submit(DNSPoisoning_ErrorCode_Distribute, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11\\DNS_SERVER_DIST'),
      executor.submit(DNSPoisoning_ErrorCode_Distribute_ProviderRegion, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11\\DNS_SERVER_DIST'),
      executor.submit(distribution_NXDomain, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
      executor.submit(distribution_NXDomain_exclude_yandex, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
      executor.submit(distribution_NoAnswer, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
      executor.submit(distribution_NoNameservers, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
      executor.submit(distribution_Timeout, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
      executor.submit(access_timely_distribution, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),

      executor.submit(DNSPoisoning_ErrorCode_Distribute, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1\\DNS_SERVER_DIST'),
      executor.submit(DNSPoisoning_ErrorCode_Distribute_ProviderRegion, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1\\DNS_SERVER_DIST'),
      executor.submit(distribution_NXDomain, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
      executor.submit(distribution_NXDomain_exclude_yandex, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
      executor.submit(distribution_NoAnswer, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
      executor.submit(distribution_NoNameservers, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
      executor.submit(distribution_Timeout, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
      executor.submit(access_timely_distribution, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
    ]

    for task in tasks:
      task.result()

  print('All tasks completed.')

