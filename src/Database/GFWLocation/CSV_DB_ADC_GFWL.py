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

def process_files(folder_location: str, file_extension: str, db_handler: MongoDBHandler, parse_function, file_format: str) -> list:
  readingResults = []
  db_handler.drop()
  file_list = os.listdir(folder_location)
  for file_name in tqdm(file_list, desc=f'Processing files'):
    if file_name.endswith(file_extension):
      with open(os.path.join(folder_location, file_name), 'r', encoding='utf-8') as file:
        results = parse_function(file)
        readingResults.extend(results)
  # 写入CSV文件
  output_file = os.path.splitext(file_name)[0] + '_processed.csv'
  write_csv(readingResults, output_file, file_format)
  return merge_results(readingResults)

def parse_csv(file):
  results = []
  try:
    first_line = ''
    # 获取第一行非空行以检测格式
    for line in file:
      if line.strip():
        first_line = line.strip()
        break
    file.seek(0)  # 重置文件指针

    if first_line.startswith('{'):
      # JSON格式解析
      for line in file:
        if not line.strip() or not line.strip().startswith('{'):
          continue  # 跳过空行或非字典格式的行
        try:
          record = ast.literal_eval(line.strip())
          domain = record.get('domain', '')
          result = record.get('result', '')
          ips = record.get('ips', {})
          error = record.get('error', '')

          if result.startswith('No GFW detected'):
            mark = 'Reached'
          elif ips:
            mark = 'lost'
          else:
            mark = 'Unknown'

          formatted_document = {
            "domain": domain,
            "ips": ";".join(ips.get('ipv4', [])),
            "error": error,
            "mark": mark
          }
          results.append(formatted_document)
        except Exception as e:
          logger.error(f'Error parsing line in CSV {file.name}: {e} | Line Content: {line.strip()}')
    else:
      # 标准CSV格式解析
      csv_reader = csv.reader(file)
      for row in csv_reader:
        if len(row) < 5 or row[0] == 'timestamp':
          continue
        ipv4 = None
        ipv6 = None
        results_ip = row[2]
        if ':' in results_ip:
          ipv6 = results_ip
        else:
          ipv4 = results_ip
        formatted_document = {
          "domain": row[1],
          "timestamp": row[0],
          "IPv4": ipv4,
          "IPv6": ipv6,
          "is_accessible": row[4]
        }
        results.append(formatted_document)
  except Exception as e:
    logger.error(f'Error while parsing CSV {file.name}: {e}')
  return results

def parse_txt(file):
  lines = file.read().strip().split('\n')
  results = []
  for line in lines:
    domain = line.split(':')[0]
    result = line.split(':')[1].strip()
    if result.startswith('No GFW detected'):
      result = result.split('(')[1].strip(')')
    else:
      result = "Not Found"
    formatted_document = {
      "domain": domain,
      "results": result
    }
    results.append(formatted_document)
  return results

def merge_results(readingResults):
  merged_results = {}
  for result in readingResults:
    domain = result['domain']
    if domain not in merged_results:
      merged_results[domain] = {key: [] for key in result.keys()}
      merged_results[domain]['domain'] = domain
    for key, value in result.items():
      if value and value not in merged_results[domain][key]:
        merged_results[domain][key].append(value)
  return list(merged_results.values())

def insert_to_db(results: list, db_handler: MongoDBHandler):
  if results:
    for result in tqdm(results, desc='Inserting to DB'):
      db_handler.insert_one(result)

def write_csv(results, output_file, file_format):
  if file_format == 'json':
    fieldnames = ['domain', 'ips', 'error', 'mark']
  else:
    fieldnames = ['domain', 'timestamp', 'IPv4', 'IPv6', 'is_accessible']

  with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for entry in results:
      if file_format == 'json':
        writer.writerow({
          'domain': entry.get('domain', ''),
          'ips': entry.get('ips', ''),
          'error': entry.get('error', ''),
          'mark': entry.get('mark', '')
        })
      else:
        writer.writerow({
          'domain': entry.get('domain', ''),
          'timestamp': entry.get('timestamp', ''),
          'IPv4': entry.get('IPv4', ''),
          'IPv6': entry.get('IPv6', ''),
          'is_accessible': entry.get('is_accessible', '')
        })

def main():
  with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # China Mobile
    CM_GFWL_results = executor.submit(
      process_files,
      os.path.join(AfterDomainChangeFolder, 'China-Mobile', 'GFWLocation/'),
      '.csv',
      CM_GFWL_ADC,
      parse_csv,
      'json'  # 指定文件格式
    )
    # China Telecom
    CT_GFWL_results = executor.submit(
      process_files,
      os.path.join(AfterDomainChangeFolder, 'China-Telecom', 'GFWDeployed/'),
      '.txt',
      CT_GFWL_ADC,
      parse_txt,
      'standard'  # 指定文件格式
    )
    CT_IPB_results = executor.submit(
      process_files,
      os.path.join(AfterDomainChangeFolder, 'China-Telecom', 'IPBlocking/'),
      '.csv',
      CT_IPB_ADC,
      parse_csv,
      'json'  # 指定文件格式
    )
    # Compare Group
    UCD_GFWL_results = executor.submit(
      process_files,
      os.path.join(AfterDomainChangeFolder, 'UCDavis-Server', 'GFWLocation/'),
      '.txt',
      UCD_GFWL_ADC,
      parse_txt,
      'standard'  # 指定文件格式
    )
    UCD_IPB_results = executor.submit(
      process_files,
      os.path.join(AfterDomainChangeFolder, 'UCDavis-Server', 'IPBlocking/'),
      '.csv',
      UCD_IPB_ADC,
      parse_csv,
      'json'  # 指定文件格式
    )

    # Insert to DB
    insert_to_db(CM_GFWL_results.result(), CM_GFWL_ADC)
    insert_to_db(CT_GFWL_results.result(), CT_GFWL_ADC)
    insert_to_db(CT_IPB_results.result(), CT_IPB_ADC)
    insert_to_db(UCD_GFWL_results.result(), UCD_GFWL_ADC)
    insert_to_db(UCD_IPB_results.result(), UCD_IPB_ADC)

if __name__ == '__main__':
  main()
