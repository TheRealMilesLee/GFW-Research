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
adc_2025_Jan_DNS = MongoDBHandler(
    ADC_db["ChinaMobile-DNSPoisoning-2025-January"])

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


def plot_pie_chart_helper(location_counts, title, output_file):
  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)
  wedges, texts, autotexts = ax.pie(location_counts.values(),
                                    labels=location_counts.keys(),
                                    autopct='%1.1f%%',
                                    startangle=140,
                                    pctdistance=0.85)
  plt.setp(autotexts, size=10, weight="bold", color="white")
  plt.setp(texts, size=10)
  ax.set_title(title)
  fig.savefig(output_file, bbox_inches='tight')
  plt.close(fig)


def plot_error_code_distribution_helper(error_code_count, title, output_file):
  # Remove entries with zero count and empty or [] data
  error_code_count = {
      k: v
      for k, v in error_code_count.items() if v > 0 and k not in ["", "[]"]
  }

  fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
  error_codes = list(error_code_count.keys())
  counts = list(error_code_count.values())
  total = sum(counts)
  ax.bar(error_codes, counts, color='skyblue')
  for i, count in enumerate(counts):
    percentage = (count / total) * 100
    ax.text(i,
            count,
            f"{count}\n({percentage:.1f}%)",
            ha='center',
            va='bottom')
  ax.set_xlabel('Error Code')
  ax.set_ylabel('Number of Occurrences')
  ax.set_title(f"{title} (Total: {total} occurrences)")
  plt.setp(ax.get_xticklabels(), rotation=25)
  fig.savefig(output_file)
  plt.close(fig)


def get_timely_trend():
  """
  绘制域名可访问性随时间的变化趋势。基于每一个DNS服务器的数据
  """
  collections = [
      "China-Mobile-DNSPoisoning", "China-Telecom-DNSPoisoning",
      "ChinaMobile-DNSPoisoning-November",
      "ChinaMobile-DNSPoisoning-2025-January"
  ]

  for dns_server in ip_to_provider.keys():
    for collection_name in collections:
      collection = ADC_db[collection_name]
      cursor = collection.find({"dns_server": dns_server})
      # 默认字典，用于按时间聚合数据
      aggregated_data = defaultdict(lambda: {
          "accessible": set(),
          "inaccessible": set()
      })

      for doc in cursor:
        timestamp = doc.get("timestamp")
        domain = doc.get("domain")
        ips = doc.get("ips", "[]")

        # 时间精确到每四个小时
        date = timestamp[:
                         10] + f" {int(timestamp[11:13]) // 4 * 4:02}:00:00" if timestamp else None

        # 确保 domain 是字符串或可哈希类型
        if isinstance(domain, list):
          domain = ",".join(domain)  # 将列表合并为字符串，用逗号分隔
        elif not isinstance(domain, str):
          domain = str(domain)  # 转换为字符串

        if date and domain:
          # 解析 IP 地址
          ips_list = parse_ips(ips)

          # 判断是否可访问
          if not ips_list or all(is_private_ip(ip) for ip in ips_list):
            aggregated_data[date]["inaccessible"].add(domain)
          else:
            aggregated_data[date]["accessible"].add(domain)

      # 合并去重
      for date in aggregated_data:
        aggregated_data[date]["accessible"] = list(
            aggregated_data[date]["accessible"])
        aggregated_data[date]["inaccessible"] = list(
            aggregated_data[date]["inaccessible"])

      # 统计按时间聚合后的数据
      x = sorted(aggregated_data.keys())  # 时间轴

      # 如果是空数据，则跳过
      if not x:
        print(
            f"No data found for {collection_name} and DNS server {dns_server}"
        )
        with open("EmptyList.txt", "w") as f:
          f.write(
              f"No data found for {collection_name} and DNS server {dns_server}\n"
          )
          f.close()
        continue
      print(
          f"Total {len(x)} data points for {collection_name} and DNS server {dns_server}"
      )
      accessible = [len(aggregated_data[date]["accessible"]) for date in x]
      inaccessible = [
          len(aggregated_data[date]["inaccessible"]) for date in x
      ]

      if x:  # 绘图, 如果x为空, 则不绘制
        plt.figure(figsize=(30, 8))  # 拉长图片横向长度
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
        plt.title(
            f"Domain Accessibility Over Time ({collection_name} - {dns_server})"
        )
        plt.legend()
        plt.xticks(rotation=25, fontsize=5)
        plt.gca().set_xticks(x)  # 设置X轴刻度与数据点匹配
        plt.tight_layout()

        # 保存图像
        if os.name == "nt":
          output_folder = "E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\"
        elif os.name == "posix":
          output_folder = "/home/silverhand/Developer/SourceRepo/GFW-Research/Pic"

        # 创建文件夹如果不存在
        folder_path = os.path.join(output_folder, collection_name)
        if not os.path.exists(folder_path):
          os.makedirs(folder_path)
        output_file = f"{folder_path}/{dns_server}_accessibility_plot.png"
        plt.savefig(output_file, dpi=300)
        plt.close()


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
        if isinstance(error_code, list):
          for code in error_code:
            error_code_count[str(code)] += 1
      else:
        error_code_count[str(error_code)] += 1

    if not error_code_count:
      print(f'No error codes found for DNS server: {server}')
      continue  # 跳过没有错误代码的服务器

    sanitized_server = server.replace(':', '_').replace('/', '_')
    output_file = f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_{sanitized_server}_{provider}.png'
    plot_error_code_distribution_helper(
        error_code_count,
        f'Error Code Distribution for DNS Server {server} ({provider})',
        output_file)


def DNSPoisoning_ErrorCode_Distribute_ProviderRegion(destination_db,
                                                     output_folder):
  """
  按区域绘制错误代码分布，以对比中国区与其他区域的DNS服务商在错误代码上的差异。
  """
  print('Plotting error code distribution by provider region...')
  region_to_error_code_count = defaultdict(Counter)

  for server, region in ip_to_region.items():
    docs = destination_db.find({'dns_server': server})
    for doc in docs:
      error_code = doc.get('error_code')
      if error_code:
        if isinstance(error_code, list):
          for code in error_code:
            region_to_error_code_count[region][str(code)] += 1
        else:
          region_to_error_code_count[region][str(error_code)] += 1

  for region, error_code_count in region_to_error_code_count.items():
    if not error_code_count:
      print(f'No error codes found for region: {region}')
      continue

    sanitized_region = region.replace(':', '_').replace('/', '_')
    output_file = f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_{sanitized_region}.png'
    plot_error_code_distribution_helper(
        error_code_count, f'Error Code Distribution for {region}',
        output_file)


def distribution_error_code(destination_db, output_folder):
  """
  绘制每个错误代码的DNS服务器占比分布图
  """
  error_code_to_server_count = defaultdict(Counter)
  all_error_codes = set()

  for server in ip_to_provider.keys():
    docs = destination_db.find({'dns_server': server})
    for doc in docs:
      error_code = doc.get('error_code')
      if error_code:
        if isinstance(error_code, list):
          for code in error_code:
            error_code_to_server_count[str(code)][ip_to_region[server]] += 1
            all_error_codes.add(str(code))
        else:
          error_code_to_server_count[str(error_code)][
              ip_to_region[server]] += 1
          all_error_codes.add(str(error_code))

  for error_code in all_error_codes:
    server_count = error_code_to_server_count[error_code]
    if not server_count:
      print(f'No servers found for error code: {error_code}')
      continue

    output_file = f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_{error_code}.png'
    plot_pie_chart_helper(
        server_count, f'DNS Servers Distribution for Error Code {error_code}',
        output_file)


if __name__ == "__main__":

  def ensure_folder_exists(folder_path):
    if not os.path.exists(folder_path):
      os.makedirs(folder_path)

  def execute_tasks(executor, tasks):
    for task in tasks:
      task.result()

  get_timely_trend()

  if os.name == "posix":
    output_folder = "/home/silverhand/Developer/SourceRepo/GFW-Research/Pic"
  elif os.name == "nt":
    output_folder = "E:\\Developer\\SourceRepo\\GFW-Research\\Pic"

  ensure_folder_exists(output_folder)
  ensure_folder_exists(f"{output_folder}/2024-9/DNS_SERVER_DIST")
  ensure_folder_exists(f"{output_folder}/2024-11/DNS_SERVER_DIST")
  ensure_folder_exists(f"{output_folder}/2025-1/DNS_SERVER_DIST")
  with ThreadPoolExecutor() as executor:
    tasks = [
        executor.submit(DNSPoisoning_ErrorCode_Distribute, DNSPoisoning,
                        f"{output_folder}/2024-9/DNS_SERVER_DIST"),
        executor.submit(DNSPoisoning_ErrorCode_Distribute,
                        merged_2024_Nov_DNS,
                        f"{output_folder}/2024-11/DNS_SERVER_DIST"),
        executor.submit(DNSPoisoning_ErrorCode_Distribute, adc_2025_Jan_DNS,
                        f"{output_folder}/2025-1/DNS_SERVER_DIST"),
        executor.submit(DNSPoisoning_ErrorCode_Distribute_ProviderRegion,
                        DNSPoisoning, f"{output_folder}/2024-9"),
        executor.submit(DNSPoisoning_ErrorCode_Distribute_ProviderRegion,
                        merged_2024_Nov_DNS, f"{output_folder}/2024-11"),
        executor.submit(DNSPoisoning_ErrorCode_Distribute_ProviderRegion,
                        adc_2025_Jan_DNS, f"{output_folder}/2025-1"),
        executor.submit(distribution_error_code, DNSPoisoning,
                        f"{output_folder}/2024-9"),
        executor.submit(distribution_error_code, merged_2024_Nov_DNS,
                        f"{output_folder}/2024-11"),
        executor.submit(distribution_error_code, adc_2025_Jan_DNS,
                        f"{output_folder}/2025-1")
    ]
    execute_tasks(executor, tasks)

  print("All tasks completed.")
