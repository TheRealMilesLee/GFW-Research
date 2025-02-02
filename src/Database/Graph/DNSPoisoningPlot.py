import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from ..DBOperations import Merged_db, MongoDBHandler, ADC_db
from datetime import datetime
from collections import defaultdict, Counter
import csv
import os
import matplotlib.dates as mdates
from ipaddress import ip_network, ip_address
import re
from concurrent.futures import ThreadPoolExecutor

# Merged_db constants
DNSPoisoning = MongoDBHandler(Merged_db["DNSPoisoning"])
merged_2024_Nov_DNS = MongoDBHandler(Merged_db["2024_Nov_DNS"])
merged_2025_Jan_DNS = MongoDBHandler(Merged_db["2025_DNS"])

categories = DNSPoisoning.distinct("error_code")

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
    "Tencent DNS Alternative": "China DNS Provider",
}

def read_dns_servers_csv():
  ip_to_provider = {}
  ip_to_region = {}
  if os.name == "nt":
    folder_location = "E:\\Developer\\SourceRepo\\GFW-Research\\src\\Import\\dns_servers.csv"
  elif os.name == "posix":
    folder_location = "/home/silverhand/Developer/SourceRepo/GFW-Research/src/Import/dns_servers.csv"
  with open(folder_location, "r") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      provider = row["Provider"]
      region = provider_region_map.get(provider, "Other DNS Provider")
      if row["IPV4"]:
        ip_to_provider[row["IPV4"]] = provider
        ip_to_region[row["IPV4"]] = region
      if row["IPV6"]:
        ip_to_provider[row["IPV6"]] = provider
        ip_to_region[row["IPV6"]] = region
  return ip_to_provider, ip_to_region

ip_to_provider, ip_to_region = read_dns_servers_csv()

def parse_ips(ips_string):
  """
    解析 IP 字符串，将其转化为 IP 列表。
    支持多种格式：
    1. 单个 IPv6 或 IPv4 地址: ['2a03:2880:f127:83:face:b00c:0:25de']
    2. 多个 IP: ['202.160.128.205']['2a03:2880:f10c:83:face:b00c:0:25de']
    3. 数组内的多个 IP 地址: ['111.7.202.175', '111.7.203.227']
    """
  if not ips_string or ips_string == "[]":
    return []  # 空字符串直接返回空列表

  # 检查是否包含嵌套数组格式
  if isinstance(ips_string, list):
    # 对数组中的每个元素调用parse_ips进行递归解析
    ip_list = []
    for sublist in ips_string:
      ip_list.extend(parse_ips(sublist))  # 递归处理
    return ip_list

  # 处理字符串类型的IPs
  pattern = r"\[([^\[\]]+)\]"
  matches = re.findall(pattern, ips_string)

  ip_list = []
  for match in matches:
    ip_list.extend(match.split(", "))

  # 去掉多余的引号和空格
  ip_list = [ip.strip("'\" ") for ip in ip_list]
  return ip_list


def is_private_ip(ip):
  """
    检查 IP 是否为内网地址
    """
  private_blocks = [
      ip_network("10.0.0.0/8"),
      ip_network("172.16.0.0/12"),
      ip_network("192.168.0.0/16"),
      ip_network("127.0.0.0/8"),  # 本地回环地址
      ip_network("169.254.0.0/16"),  # 链路本地地址
      ip_network("::1/128"),  # IPv6本地回环地址
      ip_network("fc00::/7"),  # IPv6内网地址
      ip_network("fe80::/10"),  # IPv6链路本地地址
  ]

  try:
    ip_addr = ip_address(ip)
    return any(ip_addr in block for block in private_blocks)
  except ValueError:
    # 如果 IP 无法解析，则认为不是有效地址
    return False


def process_data_and_plot():
  """
    根据多个 MongoDB 集合处理数据并绘图。
    """
  collections = [
      "China-Mobile-DNSPoisoning", "China-Telecom-DNSPoisoning",
      "ChinaMobile-DNSPoisoning-November",
      "ChinaMobile-DNSPoisoning-2025-January"
  ]

  for collection_name in collections:
    collection = ADC_db[collection_name]
    cursor = collection.find()
    # 默认字典，用于按时间聚合数据
    aggregated_data = defaultdict(lambda: {
        "accessible": set(),
        "inaccessible": set()
    })

    for doc in cursor:
      timestamp = doc.get("timestamp")
      domain = doc.get("domain")
      dns_server = doc.get("dns_server")
      ips = doc.get("ips", "[]")

      # 时间精确到每12小时
      date = timestamp[:13] + ":00:00" if timestamp else None

      # 确保 domain 和 dns_server 是字符串或可哈希类型
      if isinstance(domain, list):
        domain = ",".join(domain)  # 将列表合并为字符串，用逗号分隔
      elif not isinstance(domain, str):
        domain = str(domain)  # 转换为字符串

      if isinstance(dns_server, list):
        dns_server = ",".join(dns_server)  # 将列表合并为字符串，用逗号分隔
      elif not isinstance(dns_server, str):
        dns_server = str(dns_server)  # 转换为字符串

      if date and domain and dns_server:
        # 解析 IP 地址
        ips_list = parse_ips(ips)

        # 判断是否可访问
        if not ips_list or all(is_private_ip(ip) for ip in ips_list):
          aggregated_data[date]["inaccessible"].add((domain, dns_server))
        else:
          aggregated_data[date]["accessible"].add((domain, dns_server))

    # 统计按时间聚合后的数据
    x = sorted(aggregated_data.keys())  # 时间轴
    print(f"Total {len(x)} data points for {collection_name}")
    accessible = [len(aggregated_data[date]["accessible"]) for date in x]
    inaccessible = [len(aggregated_data[date]["inaccessible"]) for date in x]

    # 绘图
    plt.figure(figsize=(25, 6))  # 拉长图片横向长度
    plt.plot(x,
             accessible,
             label="Accessible Domains",
             color="green",
             marker="o",
             linestyle="--")
    plt.plot(x,
             inaccessible,
             label="Inaccessible Domains",
             color="red",
             marker="o",
             linestyle="--")
    plt.xlabel("Time (hourly)")
    plt.ylabel("Number of Domains")
    plt.title(f"Domain Accessibility Over Time ({collection_name})")
    plt.legend()
    plt.xticks(rotation=45, fontsize=8)
    plt.gca().xaxis.set_major_locator(
        mdates.HourLocator(interval=6))  # 设置X轴间距
    plt.tight_layout()

    # 保存图像
    output_folder = "E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\"
    output_file = f"{output_folder}{collection_name}_accessibility_plot.png"
    plt.savefig(output_file, dpi=300)
    print(f"Plot saved to: {output_file}")
    plt.close()  # 显式关闭图表以释放内存

def plot_error_code_distribution(error_code_count, title, output_file):
  fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
  error_codes = list(error_code_count.keys())
  counts = list(error_code_count.values())
  ax.bar(error_codes, counts, color='skyblue')
  for i, count in enumerate(counts):
    ax.text(i, count, str(count), ha='center', va='bottom')
  ax.set_xlabel('Error Code')
  ax.set_ylabel('Number of Occurrences')
  ax.set_title(title)
  plt.setp(ax.get_xticklabels(), rotation=45)
  total = sum(counts)
  ax.text(0.5, 1.05, f'Total: {total} occurrences', transform=ax.transAxes, ha='center')
  fig.savefig(output_file)
  plt.close(fig)

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
        error_code_str = str(error_code) if isinstance(error_code,
                                                       list) else error_code
        error_code_count[error_code_str] += 1

    if not error_code_count:
      print(f'No error codes found for DNS server: {server}')
      continue  # 跳过没有错误代码的服务器

    sanitized_server = server.replace(':', '_').replace('/', '_')
    output_file = f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_{sanitized_server}_{provider}.png'
    plot_error_code_distribution(error_code_count, f'Error Code Distribution for DNS Server {server} ({provider})', output_file)

def DNSPoisoning_ErrorCode_Distribute_ProviderRegion(destination_db,
                                                     output_folder):
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
            error_code_str = str(error_code) if isinstance(
                error_code, list) else error_code
            error_code_count[error_code_str] += 1

      if not error_code_count:
        print(f'No error codes found for region: {region}')
        continue

      sanitized_region = region.replace(':', '_').replace('/', '_')
      output_file = f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_{sanitized_region}.png'
      plot_error_code_distribution(error_code_count, f'Error Code Distribution for {region}', output_file)
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

def plot_pie_chart(location_counts, title, output_file):
  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)
  wedges, texts, autotexts = ax.pie(location_counts.values(), labels=location_counts.keys(), autopct='%1.1f%%', startangle=140, pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  ax.set_title(title)
  fig.savefig(output_file, bbox_inches='tight')
  plt.close(fig)

def distribution_error_code(destination_db, output_folder, error_code, exclude_yandex=False):
  docs = destination_db.find({"error_code": error_code}, {"error_reason": 1})
  location_counts = Counter()
  for doc in docs:
    error_reason = doc.get('error_reason', '')
    if isinstance(error_reason, list):
      error_reason = ' '.join(error_reason)
    match = re.search(r'on server:\s*(.*)$', error_reason)
    if match:
      server = match.group(1).strip()
      location = get_server_location(server)
      if not exclude_yandex or (location != 'Yandex DNS' and location != 'Yandex DNS (Additional)'):
        location_counts[location] += 1
  title = f'{error_code} Error Reason Distribution by Location'
  if exclude_yandex:
    title += ' (Yandex Excluded)'
  output_file = f'{output_folder}/{error_code}_Distribution_by_Location{"_Yandex_excluded" if exclude_yandex else ""}.png'
  plot_pie_chart(location_counts, title, output_file)

def distribution_NXDomain(destination_db, output_folder):
  distribution_error_code(destination_db, output_folder, "NXDOMAIN")

def distribution_NXDomain_exclude_yandex(destination_db, output_folder):
  distribution_error_code(destination_db, output_folder, "NXDOMAIN", exclude_yandex=True)

def distribution_NoAnswer(destination_db, output_folder):
  distribution_error_code(destination_db, output_folder, "NoAnswer")

def distribution_NoNameservers(destination_db, output_folder):
  distribution_error_code(destination_db, output_folder, "NoNameservers")

def distribution_Timeout(destination_db, output_folder):
  distribution_error_code(destination_db, output_folder, "Timeout")

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
    for ts in timestamp:
      date = datetime.fromisoformat(ts).replace(hour=0, minute=0, second=0, microsecond=0)
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
  ax.plot(dates, accessible_counts, label='Accessible', color='green')
  ax.plot(dates, inaccessible_counts, label='Inaccessible', color='red')
  ax.set_xlabel('TimeStamp')
  ax.set_ylabel('Number of Domains')
  ax.set_title('Timely Distribution of Accessible and Inaccessible Domains')
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
  ax.plot(dates, accessible_counts, label='Accessible', color='green')
  ax.plot(dates, inaccessible_counts, label='Inaccessible', color='red')
  ax.plot(dates, sometimes_counts, label='Sometimes Accessible', color='orange')
  ax.set_xlabel('Timestamp')
  ax.set_ylabel('Number of domains')
  ax.set_title('Distribution of Accessible, Inaccessible, and Sometimes Accessible Domains')
  ax.legend()

  # 格式化X轴为日期
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
  ax.xaxis.set_major_locator(mdates.AutoDateLocator())

  plt.setp(ax.get_xticklabels(), rotation=45)
  fig.savefig('access_inaccess_distribution.png')
  plt.close(fig)

if __name__ == "__main__":

  def ensure_folder_exists(folder_path):
    if not os.path.exists(folder_path):
      os.makedirs(folder_path)

  def execute_tasks(executor, tasks):
    for task in tasks:
      task.result()

  if os.name == "nt":
    folders = [
        "E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9\\DNS_SERVER_DIST",
        "E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9",
        "E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11\\DNS_SERVER_DIST",
        "E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11",
        "E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1\\DNS_SERVER_DIST",
        "E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1",
    ]
    for folder in folders:
      ensure_folder_exists(folder)

    process_data_and_plot()

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

      execute_tasks(executor, tasks)

    print("All tasks completed.")
  elif os.name == "posix":
    folders = [
        "/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-9/DNS_SERVER_DIST",
        "/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-9",
        "/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-11/DNS_SERVER_DIST",
        "/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-11",
        "/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2025-1/DNS_SERVER_DIST",
        "/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2025-1",
    ]
    for folder in folders:
      ensure_folder_exists(folder)

    process_data_and_plot()

    with ThreadPoolExecutor() as executor:
      tasks = [
        executor.submit(DNSPoisoning_ErrorCode_Distribute, DNSPoisoning, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-9/DNS_SERVER_DIST'),
        executor.submit(DNSPoisoning_ErrorCode_Distribute_ProviderRegion, DNSPoisoning, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-9/DNS_SERVER_DIST'),
        executor.submit(distribution_NXDomain, DNSPoisoning, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-9'),
        executor.submit(distribution_NXDomain_exclude_yandex, DNSPoisoning, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-9'),
        executor.submit(distribution_NoAnswer, DNSPoisoning, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-9'),
        executor.submit(distribution_NoNameservers, DNSPoisoning, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-9'),
        executor.submit(distribution_Timeout, DNSPoisoning, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-9'),
        executor.submit(access_timely_distribution, DNSPoisoning, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-9'),

        executor.submit(DNSPoisoning_ErrorCode_Distribute, merged_2024_Nov_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-11/DNS_SERVER_DIST'),
        executor.submit(DNSPoisoning_ErrorCode_Distribute_ProviderRegion, merged_2024_Nov_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-11/DNS_SERVER_DIST'),
        executor.submit(distribution_NXDomain, merged_2024_Nov_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-11'),
        executor.submit(distribution_NXDomain_exclude_yandex, merged_2024_Nov_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-11'),
        executor.submit(distribution_NoAnswer, merged_2024_Nov_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-11'),
        executor.submit(distribution_NoNameservers, merged_2024_Nov_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-11'),
        executor.submit(distribution_Timeout, merged_2024_Nov_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-11'),
        executor.submit(access_timely_distribution, merged_2024_Nov_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2024-11'),

        executor.submit(DNSPoisoning_ErrorCode_Distribute, merged_2025_Jan_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2025-1/DNS_SERVER_DIST'),
        executor.submit(DNSPoisoning_ErrorCode_Distribute_ProviderRegion, merged_2025_Jan_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2025-1/DNS_SERVER_DIST'),
        executor.submit(distribution_NXDomain, merged_2025_Jan_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2025-1'),
        executor.submit(distribution_NXDomain_exclude_yandex, merged_2025_Jan_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2025-1'),
        executor.submit(distribution_NoAnswer, merged_2025_Jan_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2025-1'),
        executor.submit(distribution_NoNameservers, merged_2025_Jan_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2025-1'),
        executor.submit(distribution_Timeout, merged_2025_Jan_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2025-1'),
        executor.submit(access_timely_distribution, merged_2025_Jan_DNS, '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic/2025-1'),
      ]

    execute_tasks(executor, tasks)

    print("All tasks completed.")

