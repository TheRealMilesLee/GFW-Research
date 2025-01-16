import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
from ..DBOperations import Merged_db, MongoDBHandler
import re
from collections import Counter
import csv
import os
from concurrent.futures import ThreadPoolExecutor

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

# 查找数据库中包含 rst_detected 字段的文档，统计 rst_detected 字段的值的分布
def distribution_GFWL_rst_detected(destination_db, output_folder):
    total_docs = destination_db.count_documents({})
    docs_with_rst_detected = destination_db.count_documents({"rst_detected": { "$exists": True }})
    rst_detected_ratio = docs_with_rst_detected / total_docs * 100

    fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)  # 使用基于 Figure 的接口
    ax.bar(['Total Docs', 'RST Detected'], [total_docs, docs_with_rst_detected])
    ax.text('Total Docs', total_docs, total_docs, ha='center', va='bottom')
    ax.text('RST Detected', docs_with_rst_detected, docs_with_rst_detected, ha='center', va='bottom')
    ax.set_xlabel('Document Type')
    ax.set_ylabel('Number of Documents')
    ax.set_title(f'RST Detected Distribution (Ratio: {rst_detected_ratio:.2f}%)')
    fig.savefig(f'{output_folder}/RST_Detected_Distribution.png')
    plt.close(fig)  # 确保图形被关闭

# 查找数据库中包含redirection_detected字段的文档，统计redirection_detected字段的值的分布
def distribution_GFWL_redirection_detected(destination_db, output_folder):
    total_docs = destination_db.count_documents({})
    docs_with_redirection_detected = destination_db.count_documents({"redirection_detected": { "$elemMatch": { "$exists": True } }})
    redirection_detected_ratio = docs_with_redirection_detected / total_docs * 100

    fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)  # 使用基于 Figure 的接口
    ax.bar(['Total Docs', 'Redirection Detected'], [total_docs, docs_with_redirection_detected])
    ax.text('Total Docs', total_docs, total_docs, ha='center', va='bottom')
    ax.text('Redirection Detected', docs_with_redirection_detected, docs_with_redirection_detected, ha='center', va='bottom')
    ax.set_xlabel('Document Type')
    ax.set_ylabel('Number of Documents')
    ax.set_title(f'Redirection Detected Distribution (Ratio: {redirection_detected_ratio:.2f}%)')
    fig.savefig(f'{output_folder}/Redirection_Detected_Distribution.png')
    plt.close(fig)  # 确保图形被关闭

# 查找数据库中Error字段的文档, 统计Error字段的值的分布
def distribution_GFWL_Error(destination_db, output_folder):
    total_docs = destination_db.count_documents({})
    docs_with_error = destination_db.count_documents({"error": { "$elemMatch": { "$exists": True } }})
    error_ratio = docs_with_error / total_docs * 100

    error_counts = Counter()
    docs = destination_db.find({"error": { "$elemMatch": { "$exists": True } }})
    for doc in docs:
        for error in doc['error']:
            error_counts[error] += 1

    fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)  # 使用基于 Figure 的接口
    ax.bar(error_counts.keys(), error_counts.values())
    for error, count in error_counts.items():
        ax.text(error, count, str(count), ha='center', va='bottom')
    ax.set_xlabel('Error')
    ax.set_ylabel('Number of Occurrences')
    ax.set_title(f'Error Distribution (Ratio: {error_ratio:.2f}%)')
    fig.savefig(f'{output_folder}/Error_Distribution.png', bbox_inches='tight')
    plt.close(fig)  # 确保图形被关闭

# 查找数据库中包含invalid_ip字段的文档, 统计其不为空的文档占总文档数量的比例
def distribution_GFWL_invalid_ip(destination_db, output_folder):
    total_docs = destination_db.count_documents({})
    docs_with_invalid_ip = destination_db.count_documents({"invalid_ip": { "$exists": True }})
    invalid_ip_ratio = docs_with_invalid_ip / total_docs * 100

    fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)  # 使用基于 Figure 的接口
    ax.bar(['Valid IP', 'Invalid IP'], [total_docs - docs_with_invalid_ip, docs_with_invalid_ip])
    ax.text('Valid IP', total_docs - docs_with_invalid_ip, total_docs - docs_with_invalid_ip, ha='center', va='bottom')
    ax.text('Invalid IP', docs_with_invalid_ip, docs_with_invalid_ip, ha='center', va='bottom')
    ax.set_xlabel('IP Type')
    ax.set_ylabel('Number of Occurrences')
    ax.set_title(f'IP Type Distribution (Ratio: {invalid_ip_ratio:.2f}%)')
    fig.savefig(f'{output_folder}/IP_Type_Distribution.png')
    plt.close(fig)  # 确保图形被关闭

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
        executor.submit(distribution_NXDomain, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),
        executor.submit(distribution_NXDomain_exclude_yandex, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),
        executor.submit(distribution_NoAnswer, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),
        executor.submit(distribution_NoNameservers, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),
        executor.submit(distribution_Timeout, DNSPoisoning, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9'),

        executor.submit(DNSPoisoning_ErrorCode_Distribute, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11\\DNS_SERVER_DIST'),
        executor.submit(distribution_NXDomain, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
        executor.submit(distribution_NXDomain_exclude_yandex, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
        executor.submit(distribution_NoAnswer, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
        executor.submit(distribution_NoNameservers, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
        executor.submit(distribution_Timeout, merged_2024_Nov_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
        executor.submit(distribution_GFWL_rst_detected, merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
        executor.submit(distribution_GFWL_redirection_detected, merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
        executor.submit(distribution_GFWL_Error, merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),
        executor.submit(distribution_GFWL_invalid_ip, merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11'),

        executor.submit(DNSPoisoning_ErrorCode_Distribute, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1\\DNS_SERVER_DIST'),
        executor.submit(distribution_NXDomain, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
        executor.submit(distribution_NXDomain_exclude_yandex, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
        executor.submit(distribution_NoAnswer, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
        executor.submit(distribution_NoNameservers, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
        executor.submit(distribution_Timeout, merged_2025_Jan_DNS, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
        executor.submit(distribution_GFWL_rst_detected, merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
        executor.submit(distribution_GFWL_redirection_detected, merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
        executor.submit(distribution_GFWL_Error, merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'),
        executor.submit(distribution_GFWL_invalid_ip, merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
    ]

    for task in tasks:
        task.result()

  print('All tasks completed.')

