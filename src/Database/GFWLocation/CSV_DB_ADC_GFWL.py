import ast
import concurrent.futures
import csv
import logging
import multiprocessing
import os
import os.path

import tqdm
from ..DBOperations import ADC_db, MongoDBHandler
from tqdm import tqdm

# TestResults
CM_GFWL_ADC = MongoDBHandler(ADC_db['China-Mobile-GFWLocation'])
CT_GFWL_ADC = MongoDBHandler(ADC_db['China-Telecom-GFWLocation'])
CT_IPB_ADC = MongoDBHandler(ADC_db['China-Telecom-IPBlocking'])

# CompareGroup
UCD_GFWL_ADC = MongoDBHandler(ADC_db['UCDavis-Server-GFWLocation'])
UCD_IPB_ADC = MongoDBHandler(ADC_db['UCDavis-Server-IPBlocking'])

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

# Traceroute results
def CT_IPB(folder_location: str) -> list:
  readingResults = []
  CT_IPB_ADC.drop()
  file_list = os.listdir(folder_location)
  for file_name in tqdm(file_list, desc='Processing China-Telecom-IPBlocking'):
    if file_name.endswith('.csv'):
      with open(folder_location + file_name, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
          if row[0] == 'timestamp':
            continue
          formatted_document = {
            "timestamp": row[0],
            "domain": row[1],
            "results_ip": row[2],
            "ip_type": row[3],
            "port": row[4],
            "is_accessible": row[5]
          }
          readingResults.append(formatted_document)
    elif file_name.endswith('.txt'):
      with open(folder_location + file_name, 'r', encoding='utf-8') as file:
        lines = file.read().strip().split('\n')
        for line in lines:
          if ":" in lines:
            domain = line.split(':')[0]
            result = line.split(':')[1].strip()
            if result.startswith('No GFW detected'):
              result = result.split('(')[1].strip(')')
            else:
              result = "Not Found"
            formatted_document = {
              "domain": domain,
              "result": result
            }
            readingResults.append(formatted_document)
  # Data merge and cleanup
  merged_results = {}
  for result in readingResults:
    domain = result['domain']
    if domain not in merged_results:
      merged_results[domain] = {
        'domain': domain,
        'timestamp': [],
        'results_ip': [],
        'ip_type': [],
        'port': [],
        'is_accessible': [],
        'problem_domain': False
      }
    if result['timestamp'] and result['timestamp'] not in merged_results[domain]['timestamp']:
      merged_results[domain]['timestamp'].append(result['timestamp'])
    if result['results_ip'] and result['results_ip'] not in merged_results[domain]['results_ip']:
      merged_results[domain]['results_ip'].append(result['results_ip'])
    if result['ip_type'] and result['ip_type'] not in merged_results[domain]['ip_type']:
      merged_results[domain]['ip_type'].append(result['ip_type'])
    if result['port'] and result['port'] not in merged_results[domain]['port']:
      merged_results[domain]['port'].append(result['port'])
    if result['is_accessible'] and result['is_accessible'] not in merged_results[domain]['is_accessible']:
      merged_results[domain]['is_accessible'].append(result['is_accessible'])

  # Mark problem domains
  for domain, data in merged_results.items():
    if not data['timestamp'] and not data['results_ip'] and not data['ip_type'] and not data['port'] and not data['is_accessible']:
      data['problem_domain'] = True

  readingResults = list(merged_results.values())
    # create unique index for domain
  CT_IPB_ADC.create_index('domain', unique=True)
  return readingResults

def CT_GFWL(folder_location: str) -> list:
  readingResults = []
  CT_GFWL_ADC.drop()
  file_list = os.listdir(folder_location)
  for file_name in tqdm(file_list, desc='Processing China-Telecom-GFWLocation'):
    if file_name.endswith('.txt'):
      with open(folder_location + file_name, 'r', encoding='utf-8') as file:
        lines = file.read().strip().split('\n')
        for line in lines:
          domain = line.split(':')[0]
          result = line.split(':')[1].strip()
          if result.startswith('No GFW detected'):
            result = result.split('(')[1].strip(')')
          else:
            result = "Not Found"
          formatted_document = {
            "domain": domain,
            "result": result
          }
          readingResults.append(formatted_document)

  # Data merge and cleanup
  merged_results = {}
  for result in readingResults:
    domain = result['domain']
    if domain not in merged_results:
      merged_results[domain] = {
        'domain': domain,
        'result': []
      }
    if result['result'] and result['result'] not in merged_results[domain]['result']:
      merged_results[domain]['result'].append(result['result'])

  readingResults = list(merged_results.values())
    # create unique index for domain
  CT_GFWL_ADC.create_index('domain', unique=True)
  return readingResults

def CM_GFWL(folder_location: str) -> list:
  readingResults = []
  CM_GFWL_ADC.drop()
  file_list = os.listdir(folder_location)
  for file_name in tqdm(file_list, desc='Processing China-Mobile-GFWLocation'):
    if file_name.endswith('.csv'):
      with open(folder_location + file_name, 'r', encoding='utf-8') as file:
        lines = file.read().strip().split('\n')
        parsed_data = [ast.literal_eval(line) for line in lines]
        fieldnames = ['domain', 'result', 'error', 'ipv4', 'ipv6', 'location']
        for entry in parsed_data:
          # 提取数据并格式化
          row = {
            'domain': entry.get('domain'),
            'result': entry.get('result'),
            'error': entry.get('error'),
            'ipv4': ', '.join(entry.get('ips', {}).get('ipv4', [])),
            'ipv6': ', '.join(entry.get('ips', {}).get('ipv6', [])),
            'location': entry.get('location', {}).get('ipv4 address'),
          }
          formatted_document = {
            "domain": row['domain'],
            "result": row['result'],
            "error": row['error'],
            "answers": row['ipv4'] + row['ipv6'],
            "location": row['location']
          }
          readingResults.append(formatted_document)
  # Merge and cleanup data
  merged_results = {}
  for result in readingResults:
    domain = result['domain']
    if domain not in merged_results:
      merged_results[domain] = {
        'domain': domain,
        'results': [],
        'error': [],
        'answers': [],
        'location': []
      }
    if result['result'] and result['result'] not in merged_results[domain]['results']:
      merged_results[domain]['results'].append(result['result'])
    if result['error'] and result['error'] not in merged_results[domain]['error']:
      merged_results[domain]['error'].append(result['error'])
    if result['answers'] and result['answers'] not in merged_results[domain]['answers']:
      merged_results[domain]['answers'].append(result['answers'])
    if result['location'] and result['location'] not in merged_results[domain]['location']:
      merged_results[domain]['location'].append(result['location'])

  readingResults = list(merged_results.values())
    # create unique index for domain
  CM_GFWL_ADC.create_index('domain', unique=True)
  return readingResults

def UCD_GFWL(folder_location: str) -> list:
  CompareGroupResults = []
  UCD_GFWL_ADC.drop()
  file_list = os.listdir(folder_location)
  for file_name in tqdm(file_list, desc='Processing UCDavis-Server-GFWLocation'):
    if file_name.endswith('.txt'):
      with open(folder_location + file_name, 'r', encoding='utf-8') as file:
        lines = file.read().strip().split('\n')
        for line in lines:
          domain = line.split(':')[0]
          result = line.split(':')[1].strip()
          if result.startswith('No GFW detected'):
            result = result.split('(')[1].strip(')')
          else:
            result = "Not Found"
          formatted_document = {
            "domain": domain,
            "result": result
          }
          CompareGroupResults.append(formatted_document)

  # Data merge and cleanup
  merged_results = {}
  for result in CompareGroupResults:
    domain = result['domain']
    if domain not in merged_results:
      merged_results[domain] = {
        'domain': domain,
        'result': []
      }
    if result['result'] and result['result'] not in merged_results[domain]['result']:
      merged_results[domain]['result'].append(result['result'])

  CompareGroupResults = list(merged_results.values())
    # create unique index for domain
  UCD_GFWL_ADC.create_index('domain', unique=True)
  return CompareGroupResults

def UCD_IPB(folder_location: str) -> list:
  CompareGroupResults = []
  UCD_IPB_ADC.drop()
  file_list = os.listdir(folder_location)
  for file_name in tqdm(file_list, desc='Processing UCDavis-Server-IPBlocking'):
    if file_name.endswith('.csv'):
      with open(folder_location + file_name, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
          if row[0] == 'timestamp':
            continue
          formatted_document = {
            "timestamp": row[0],
            "domain": row[1],
            "results_ip": row[2],
            "ip_type": row[3],
            "port": row[4],
            "is_accessible": row[5]
          }
          CompareGroupResults.append(formatted_document)
    elif file_name.endswith('.txt'):
      with open(folder_location + file_name, 'r', encoding='utf-8') as file:
        lines = file.read().strip().split('\n')
        for line in lines:
          if ":" in lines:
            domain = line.split(':')[0]
            result = line.split(':')[1].strip()
            if result.startswith('No GFW detected'):
              result = result.split('(')[1].strip(')')
            else:
              result = "Not Found"
            formatted_document = {
              "domain": domain,
              "result": result
            }
            CompareGroupResults.append(formatted_document)

  # Data merge and cleanup
  merged_results = {}
  for result in CompareGroupResults:
    domain = result['domain']
    if domain not in merged_results:
      merged_results[domain] = {
        'domain': domain,
        'timestamp': [],
        'results_ip': [],
        'ip_type': [],
        'port': [],
        'is_accessible': [],
        'problem_domain': False
      }
    if result['timestamp'] and result['timestamp'] not in merged_results[domain]['timestamp']:
      merged_results[domain]['timestamp'].append(result['timestamp'])
    if result['results_ip'] and result['results_ip'] not in merged_results[domain]['results_ip']:
      merged_results[domain]['results_ip'].append(result['results_ip'])
    if result['ip_type'] and result['ip_type'] not in merged_results[domain]['ip_type']:
      merged_results[domain]['ip_type'].append(result['ip_type'])
    if result['port'] and result['port'] not in merged_results[domain]['port']:
      merged_results[domain]['port'].append(result['port'])
    if result['is_accessible'] and result['is_accessible'] not in merged_results[domain]['is_accessible']:
      merged_results[domain]['is_accessible'].append(result['is_accessible'])

  # Mark problem domains
  for domain, data in merged_results.items():
    if not data['timestamp'] and not data['results_ip'] and not data['ip_type'] and not data['port'] and not data['is_accessible']:
      data['problem_domain'] = True

  CompareGroupResults = list(merged_results.values())
    # create unique index for domain
  UCD_IPB_ADC.create_index('domain', unique=True)
  return CompareGroupResults

def insert_to_db(results: list, db_handler: MongoDBHandler):
  if results:
    for result in tqdm(results, desc='Inserting to DB'):
      db_handler.insert_one(result)

def main():
  with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # China Mobile
    CM_GFWL_results = executor.submit(CM_GFWL, os.path.join(AfterDomainChangeFolder, 'China-Mobile', 'GFWLocation/'))
    # China Telecom
    CT_GFWL_results = executor.submit(CT_GFWL, os.path.join(AfterDomainChangeFolder, 'China-Telecom', 'GFWDeployed/'))
    CT_IPB_results = executor.submit(CT_IPB, os.path.join(AfterDomainChangeFolder, 'China-Telecom', 'IPBlocking/'))
    # Compare Group
    UCD_GFWL_results = executor.submit(UCD_GFWL, os.path.join(AfterDomainChangeFolder, 'UCDavis-Server', 'GFWLocation/'))
    UCD_IPB_results = executor.submit(UCD_IPB, os.path.join(AfterDomainChangeFolder, 'UCDavis-Server', 'IPBlocking/'))

    # Insert to DB
    insert_to_db(CM_GFWL_results.result(), CM_GFWL_ADC)
    insert_to_db(CT_GFWL_results.result(), CT_GFWL_ADC)
    insert_to_db(CT_IPB_results.result(), CT_IPB_ADC)
    insert_to_db(UCD_GFWL_results.result(), UCD_GFWL_ADC)
    insert_to_db(UCD_IPB_results.result(), UCD_IPB_ADC)

if __name__ == '__main__':
  main()
