import ast
import concurrent.futures
import csv
import logging
import multiprocessing
import os
import os.path

import tqdm
from DBOperations import ADC_db, MongoDBHandler
from tqdm import tqdm

# TestResults
CM_DNSP_ADC = MongoDBHandler(ADC_db['China-Mobile-DNSPoisoning'])
CT_DNSP_ADC = MongoDBHandler(ADC_db['China-Telecom-DNSPoisoning'])

# CompareGroup
UCD_DNSP_ADC = MongoDBHandler(ADC_db['UCDavis-Server-DNSPoisoning'])

# Optimize worker count based on CPU cores
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 64)  # Dynamically set workers

if os.name == 'nt':
  AfterDomainChangeFolder = 'E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\AfterDomainChange\\'
else:
  AfterDomainChangeFolder = '/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/AfterDomainChange/'
# 移除所有现有的处理程序
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
# 创建 logger
logger = logging.getLogger(__name__)

# DNS Poisoning results
def CM_DNSP(folder_location: str) -> list:
  readingResults = []
  CM_DNSP_ADC.drop()
  file_list = os.listdir(folder_location)
  for file_name in tqdm(file_list, desc='Processing China-Mobile-DNSPoisoning'):
    if file_name.endswith('.csv'):
      with open(folder_location + file_name, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
          if row[0] == 'timestamp':
            continue
          try:
            dns_servers = ast.literal_eval(row[2])  # 使用 ast.literal_eval 安全地将字符串转换为列表
          except (ValueError, SyntaxError):
            dns_servers = [row[2]]  # 如果转换失败，则将其视为单个 DNS 服务器
          for dns_server in dns_servers:
            formatted_document = {
              'timestamp': row[0],
              'domain': row[1],
              'dns_server': dns_server,
              'results': row[3] + row[4]
            }
            readingResults.append(formatted_document)

  # Data merge and cleanup
  merged_results = {}
  for result in readingResults:
    key = (result['domain'], result['dns_server'])  # 使用(domain, dns_server)作为唯一键
    if key not in merged_results:
      merged_results[key] = {
        'domain': result['domain'],
        'dns_server': result['dns_server'],
        'timestamp': [],
        'results': []
      }
    if result['timestamp'] and result['timestamp'] not in merged_results[key]['timestamp']:
      merged_results[key]['timestamp'].append(result['timestamp'])
    if result['results'] and result['results'] not in merged_results[key]['results']:
      merged_results[key]['results'].append(result['results'])

  readingResults = list(merged_results.values())
  CM_DNSP_ADC.create_index([('domain', 1), ('dns_server', 1)], unique=True)  # 创建复合唯一索引
  return readingResults

def CT_DNSP(folder_location: str) -> list:
  readingResults = []
  CT_DNSP_ADC.drop()
  file_list = os.listdir(folder_location)
  for file_name in tqdm(file_list, desc='Processing China-Telecom-DNSPoisoning'):
    if file_name.endswith('.csv'):
      with open(folder_location + file_name, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
          if row[0] == 'timestamp':
            continue
          try:
            dns_servers = ast.literal_eval(row[2])  # 使用 ast.literal_eval 安全地将字符串转换为列表
          except (ValueError, SyntaxError):
            dns_servers = [row[2]]  # 如果转换失败，则将其视为单个 DNS 服务器
          for dns_server in dns_servers:
            determind_poisoned = (row[7].strip().lower() == 'true') and (row[8].strip().lower() == 'true')
            formatted_document = {
              "timestamp": row[0],
              "domain": row[1],
              "dns_server": dns_server,
              "answers": row[3] + row[4] + row[5],
              "is_poisoned": determind_poisoned
            }
            readingResults.append(formatted_document)

  # Data merge and cleanup
  merged_results = {}
  for result in readingResults:
    key = (result['domain'], result['dns_server'])  # 使用(domain, dns_server)作为唯一键
    if key not in merged_results:
      merged_results[key] = {
        'domain': result['domain'],
        'dns_server': result['dns_server'],
        'timestamp': [],
        'answers': [],
        'is_poisoned': result['is_poisoned']
      }
    if result['timestamp'] and result['timestamp'] not in merged_results[key]['timestamp']:
      merged_results[key]['timestamp'].append(result['timestamp'])
    if result['answers'] and result['answers'] not in merged_results[key]['answers']:
      merged_results[key]['answers'].append(result['answers'])

  readingResults = list(merged_results.values())
  CT_DNSP_ADC.create_index([('domain', 1), ('dns_server', 1)], unique=True)  # 创建复合唯一索引
  return readingResults

def UCD_DNSP(folder_location: str) -> list:
  CompareGroupResults = []
  UCD_DNSP_ADC.drop()
  file_list = os.listdir(folder_location)
  for file_name in tqdm(file_list, desc='Processing UCDavis-Server-DNSPoisoning'):
    if file_name.endswith('.csv'):
      with open(folder_location + file_name, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
          if row[0] == 'timestamp':
            continue
          determind_poisoned = (row[7].strip().lower() == 'true') and (row[8].strip().lower() == 'true')
          formatted_document = {
            "timestamp": row[0],
            "domain": row[1],
            "dns_server": row[2],
            "answers": row[3] + row[4] + row[5],
            "is_poisoned": determind_poisoned
          }
          CompareGroupResults.append(formatted_document)

  # Data merge and cleanup
  merged_results = {}
  for result in CompareGroupResults:
    key = (result['domain'], result['dns_server'])  # 使用(domain, dns_server)作为唯一键
    if key not in merged_results:
      merged_results[key] = {
        'domain': result['domain'],
        'dns_server': result['dns_server'],
        'timestamp': [],
        'answers': [],
        'is_poisoned': result['is_poisoned']
      }
    if result['timestamp'] and result['timestamp'] not in merged_results[key]['timestamp']:
      merged_results[key]['timestamp'].append(result['timestamp'])
    if result['answers'] and result['answers'] not in merged_results[key]['answers']:
      merged_results[key]['answers'].append(result['answers'])

  CompareGroupResults = list(merged_results.values())
  UCD_DNSP_ADC.create_index([('domain', 1), ('dns_server', 1)], unique=True)  # 创建复合唯一索引
  return CompareGroupResults


def insert_to_db(results: list, db_handler: MongoDBHandler):
  if results:
    for result in tqdm(results, desc='Inserting to DB'):
      db_handler.insert_one(result)

def main():
  with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # China Mobile
    CM_DNSP_results = executor.submit(CM_DNSP, os.path.join(AfterDomainChangeFolder, 'China-Mobile', 'DNSPoisoning/'))
    # China Telecom
    CT_DNSP_results = executor.submit(CT_DNSP, os.path.join(AfterDomainChangeFolder, 'China-Telecom', 'DNSPoisoning/'))
    # Compare Group
    UCD_DNSP_results = executor.submit(UCD_DNSP, os.path.join(AfterDomainChangeFolder, 'UCDavis-Server', 'DNSPoisoning/'))

    # Insert to DB
    insert_to_db(CM_DNSP_results.result(), CM_DNSP_ADC)
    insert_to_db(CT_DNSP_results.result(), CT_DNSP_ADC)
    insert_to_db(UCD_DNSP_results.result(), UCD_DNSP_ADC)


if __name__ == '__main__':
  main()
