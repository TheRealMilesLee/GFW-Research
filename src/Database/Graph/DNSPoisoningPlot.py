import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from ..DBOperations import Merged_db, MongoDBHandler, ADC_db
from collections import defaultdict, Counter
import csv
import os
from ipaddress import ip_network, ip_address
import re
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import numpy as np
import matplotlib.pyplot as plt
# Merged_db constants
DNSPoisoning = MongoDBHandler(Merged_db["DNSPoisoning"])
merged_2024_Nov_DNS = MongoDBHandler(Merged_db["2024_Nov_DNS"])
merged_2025_Jan_DNS = MongoDBHandler(Merged_db["2025_DNS"])
adc_2025_Jan_DNS = MongoDBHandler(
    ADC_db["ChinaMobile-DNSPoisoning-2025-January"])
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 128)  # Dynamically set workers

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
  folder_location = "/home/lhengyi/Developer/GFW-Research/src/Import/dns_servers.csv"
  if os.name == "nt":
    folder_location = "E:\\Developer\\SourceRepo\\GFW-Research\\src\\Import\\dns_servers.csv"
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
  if not ips_string or ips_string == "[]":
    return []
  if isinstance(ips_string, list):
    ip_list = []
    for sublist in ips_string:
      ip_list.extend(parse_ips(sublist))
    return ip_list
  pattern = r"\[([^\[\]]+)\]"
  matches = re.findall(pattern, ips_string)
  ip_list = []
  for match in matches:
    ip_list.extend(match.split(", "))
  ip_list = [ip.strip("'\" ") for ip in ip_list]
  return ip_list


def is_private_ip(ip):
  private_blocks = [
      ip_network("10.0.0.0/8"),
      ip_network("172.16.0.0/12"),
      ip_network("192.168.0.0/16"),
      ip_network("127.0.0.0/8"),
      ip_network("169.254.0.0/16"),
      ip_network("::1/128"),
      ip_network("fc00::/7"),
      ip_network("fe80::/10"),
  ]
  try:
    ip_addr = ip_address(ip)
    return any(ip_addr in block for block in private_blocks)
  except ValueError:
    return False


def get_server_location(server):
  if server in ip_to_provider:
    return ip_to_provider[server]
  regex = re.compile(r'^(.*?)(?: Timeout| Non-existent | No)')
  ip_match = regex.match(server)
  if ip_match:
    return ip_to_provider.get(ip_match.group(1), 'Unknown')
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
  error_code_count = {
      k: v
      for k, v in error_code_count.items()
      if v > 0 and k not in ["", "[]", "erying", "refuse", "former"]
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


def plot_error_code_distribution_provider_region_stacked(
    region_to_error_code_count, title, output_file):
  print(f"Plotting error code distribution for {title}...")
  regions = sorted(region_to_error_code_count.keys())
  all_codes = sorted(
      set().union(*[c.keys() for c in region_to_error_code_count.values()]))
  x = np.arange(len(regions))
  bottom = [0] * len(regions)
  plt.figure(figsize=(12, 6))
  for code in all_codes:
    counts = [region_to_error_code_count[r].get(code, 0) for r in regions]
    plt.bar(x, counts, bottom=bottom, label=code)
    bottom = [bottom[i] + counts[i] for i in range(len(regions))]
  plt.xticks(x, regions, rotation=25)
  plt.xlabel("DNS Provider Region")
  plt.ylabel("Occurrences")
  plt.title(title)
  plt.legend()
  plt.tight_layout()
  plt.savefig(output_file)
  plt.close()


def get_timely_trend():
  collections = [
      "China-Mobile-DNSPoisoning", "China-Telecom-DNSPoisoning",
      "ChinaMobile-DNSPoisoning-November",
      "ChinaMobile-DNSPoisoning-2025-January"
  ]
  for dns_server in ip_to_provider.keys():
    for collection_name in collections:
      collection = ADC_db[collection_name]
      cursor = collection.find({"dns_server": dns_server})
      aggregated_data = defaultdict(lambda: {
          "accessible": set(),
          "inaccessible": set()
      })
      for doc in cursor:
        timestamp = doc.get("timestamp")
        domain = doc.get("domain")
        ips = doc.get("ips", "[]")
        date = timestamp[:
                         10] + f" {int(timestamp[11:13]) // 4 * 4:02}:00:00" if timestamp else None
        if isinstance(domain, list):
          domain = ",".join(domain)
        elif not isinstance(domain, str):
          domain = str(domain)
        if date and domain:
          ips_list = parse_ips(ips)
          if not ips_list or all(is_private_ip(ip) for ip in ips_list):
            aggregated_data[date]["inaccessible"].add(domain)
          else:
            aggregated_data[date]["accessible"].add(domain)
      for date in aggregated_data:
        aggregated_data[date]["accessible"] = list(
            aggregated_data[date]["accessible"])
        aggregated_data[date]["inaccessible"] = list(
            aggregated_data[date]["inaccessible"])
      x = sorted(aggregated_data.keys())
      if not x:
        print(
            f"No data found for {collection_name} and DNS server {dns_server}"
        )
        with open("EmptyList.txt", "w") as f:
          f.write(
              f"No data found for {collection_name} and DNS server {dns_server}\n"
          )
        continue
      print(
          f"Total {len(x)} data points for {collection_name} and DNS server {dns_server}"
      )
      accessible = [len(aggregated_data[date]["accessible"]) for date in x]
      inaccessible = [
          len(aggregated_data[date]["inaccessible"]) for date in x
      ]
      if x:
        plt.figure(figsize=(30, 8))
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
        plt.gca().set_xticks(x)
        plt.tight_layout()
        output_folder = "/home/lhengyi/Developer/GFW-Research/Pic"
        if os.name == "nt":
          output_folder = "E:\\Developer\\SourceRepo\\GFW-Research\\Pic"
        folder_path = os.path.join(output_folder, collection_name)
        if not os.path.exists(folder_path):
          os.makedirs(folder_path)
        output_file = f"{folder_path}/{dns_server}_accessibility_plot.png"
        plt.savefig(output_file, dpi=300)
        plt.close()


def DNSPoisoning_ErrorCode_Distribute(destination_db, output_folder):
  print('Plotting error code distribution by DNS server...')
  dns_servers = list(ip_to_provider.keys())
  for server in dns_servers:
    provider = ip_to_provider.get(server, 'Unknown Provider')
    error_code_count = Counter()
    docs = destination_db.find({'dns_server': server})
    domain_record_errors = defaultdict(lambda: defaultdict(set))
    for doc in docs:
      domain = doc.get("domain")
      record_type = doc.get("record_type")
      if domain and record_type:
        ec = doc.get("error_code", [])
        if isinstance(ec, str):
          ec = [ec]
        if record_type in ("A", "AAAA"):
          # 只暂存“NoAnswer”即可，先不做双向核验
          if "NoAnswer" in ec:
            domain_record_errors[domain][record_type].add("NoAnswer")
          for code in ec:
            if isinstance(code, list):
              code = str(code)
            if code != "NoAnswer":
              domain_record_errors[domain][record_type].add(code)
        else:
          for code in ec:
            if isinstance(code, list):
              code = str(code)
            domain_record_errors[domain][record_type].add(code)
    # 核验 A、AAAA 是否都出现了 NoAnswer
    for d, recs in domain_record_errors.items():
      if "NoAnswer" in recs.get("A", set()) or "NoAnswer" in recs.get(
          "AAAA", set()):
        if not ("NoAnswer" in recs.get("A", set())
                and "NoAnswer" in recs.get("AAAA", set())):
          recs["A"].discard("NoAnswer")
          recs["AAAA"].discard("NoAnswer")
    for d, recs in domain_record_errors.items():
      for rtype, codes in recs.items():
        for c in codes:
          error_code_count[c] += 1
    if not error_code_count:
      continue
    sanitized_server = server.replace(':', '_').replace('/', '_')
    output_file = f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_{sanitized_server}_{provider}.png'
    plot_error_code_distribution_helper(
        error_code_count,
        f'Error Code Distribution for DNS Server {server} ({provider})',
        output_file)


def DNSPoisoning_ErrorCode_Distribute_ProviderRegion(destination_db,
                                                     output_folder):
  print('Plotting error code distribution by provider region...')
  region_to_error_code_count = defaultdict(Counter)
  for server, region in ip_to_region.items():
    docs = destination_db.find({'dns_server': server})
    domain_record_errors = defaultdict(lambda: defaultdict(set))
    for doc in docs:
      domain = doc.get("domain")
      record_type = doc.get("record_type")
      if domain and record_type:
        ec = doc.get("error_code", [])
        if isinstance(ec, str):
          ec = [ec]
        if record_type in ("A", "AAAA"):
          # 只暂存“NoAnswer”即可，先不做双向核验
          if "NoAnswer" in ec:
            domain_record_errors[domain][record_type].add("NoAnswer")
          for code in ec:
            if isinstance(code, list):
              code = str(code)
            if code != "NoAnswer":
              domain_record_errors[domain][record_type].add(code)
        else:
          for code in ec:
            if isinstance(code, list):
              code = str(code)
            domain_record_errors[domain][record_type].add(code)
    # 核验 A、AAAA 是否都出现了 NoAnswer
    for d, recs in domain_record_errors.items():
      if "NoAnswer" in recs.get("A", set()) or "NoAnswer" in recs.get(
          "AAAA", set()):
        if not ("NoAnswer" in recs.get("A", set())
                and "NoAnswer" in recs.get("AAAA", set())):
          recs["A"].discard("NoAnswer")
          recs["AAAA"].discard("NoAnswer")
    for d, recs in domain_record_errors.items():
      for rtype, codes in recs.items():
        for c in codes:
          region_to_error_code_count[region][str(c)] += 1
  for region, error_code_count in region_to_error_code_count.items():
    if not error_code_count:
      print(f'No error codes found for region: {region}')
      continue
    sanitized_region = region.replace(':', '_').replace('/', '_')
    output_file = f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_{sanitized_region}.png'
    plot_error_code_distribution_helper(
        error_code_count, f'Error Code Distribution for {region}',
        output_file)


def DNSPoisoning_ErrorCode_Distribute_ProviderRegion_Aggregate(
    destination_db, output_folder):
  """
   1. 按照上一个函数一样绘制出按照DNS提供商所在的地域聚合的错误码分布图
   2. 将所有地域聚合到一张图中
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
  all_error_code_count = Counter()
  for region, error_code_count in region_to_error_code_count.items():
    all_error_code_count.update(error_code_count)
  if not all_error_code_count:
    print(f'No error codes found for all regions')
    return
  output_file = f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_All.png'
  plot_error_code_distribution_provider_region_stacked(
      region_to_error_code_count, 'Error Code Distribution for All Regions',
      output_file)


def distribution_error_code(destination_db, output_folder):
  print('Plotting DNS server distribution for each error code...')
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
    if error_code in ["erying", "refuse", "former"]:
      continue
    server_count = error_code_to_server_count[error_code]
    if not server_count:
      print(f'No servers found for error code: {error_code}')
      continue
    output_file = f'{output_folder}/DNSPoisoning_ErrorCode_Distribute_{error_code}.png'
    plot_pie_chart_helper(
        server_count, f'DNS Servers Distribution for Error Code {error_code}',
        output_file)


def ensure_folder_exists(folder_path):
  if not os.path.exists(folder_path):
    os.makedirs(folder_path)


def execute_tasks(executor, tasks):
  for task in tasks:
    task.result()


if __name__ == "__main__":
  # get_timely_trend()
  output_folder = "/home/lhengyi/Developer/GFW-Research/Pic"
  if os.name == "nt":
    output_folder = "E:\\Developer\\SourceRepo\\GFW-Research\\Pic"
  ensure_folder_exists(output_folder)
  ensure_folder_exists(f"{output_folder}/2024-9/DNS_SERVER_DIST")
  ensure_folder_exists(f"{output_folder}/2024-11/DNS_SERVER_DIST")
  ensure_folder_exists(f"{output_folder}/2025-1/DNS_SERVER_DIST")

  DNSPoisoning_ErrorCode_Distribute(
      DNSPoisoning, f"{output_folder}/2024-9/DNS_SERVER_DIST")
  DNSPoisoning_ErrorCode_Distribute(
      merged_2024_Nov_DNS, f"{output_folder}/2024-11/DNS_SERVER_DIST")
  DNSPoisoning_ErrorCode_Distribute(
      merged_2025_Jan_DNS, f"{output_folder}/2025-1/DNS_SERVER_DIST")

  DNSPoisoning_ErrorCode_Distribute_ProviderRegion(DNSPoisoning,
                                                   f"{output_folder}/2024-9")
  DNSPoisoning_ErrorCode_Distribute_ProviderRegion(
      merged_2024_Nov_DNS, f"{output_folder}/2024-11")
  DNSPoisoning_ErrorCode_Distribute_ProviderRegion(merged_2025_Jan_DNS,
                                                   f"{output_folder}/2025-1")

  DNSPoisoning_ErrorCode_Distribute_ProviderRegion_Aggregate(
      DNSPoisoning, f"{output_folder}/2024-9")
  DNSPoisoning_ErrorCode_Distribute_ProviderRegion_Aggregate(
      merged_2024_Nov_DNS, f"{output_folder}/2024-11")
  DNSPoisoning_ErrorCode_Distribute_ProviderRegion_Aggregate(
      merged_2025_Jan_DNS, f"{output_folder}/2025-1")

  distribution_error_code(DNSPoisoning, f"{output_folder}/2024-9")
  distribution_error_code(merged_2024_Nov_DNS, f"{output_folder}/2024-11")
  distribution_error_code(merged_2025_Jan_DNS, f"{output_folder}/2025-1")
  print("All tasks completed.")
