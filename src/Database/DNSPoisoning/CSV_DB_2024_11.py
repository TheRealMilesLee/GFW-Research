import concurrent.futures
import csv
import logging
import os
import multiprocessing
from collections import defaultdict
import ast

from ..DBOperations import ADC_db, MongoDBHandler
from tqdm import tqdm
CPU_CORES = multiprocessing.cpu_count()
CM_DNSP_ADC_NOV = ADC_db['ChinaMobile-DNSPoisoning-November']
MAX_WORKERS = max(CPU_CORES * 2, 64)  # Dynamically set workers
# Config Logger
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 创建 logger
logger = logging.getLogger(__name__)


def process_file(file, mongodbOP_CM_DNSP):
  with open(file, 'r') as csvfile:
    logger.info(f'Processing file: {os.path.basename(file)}')
    reader = csv.DictReader(csvfile)
    data_dict = defaultdict(list)  # 修改为存储文档列表

    # 将数据分类到字典中，准备插入
    for row in reader:
      if 'dns_server' not in row:
        logger.error(f"Missing 'dns_server' key in row: {row}")
        continue
      try:
        dns_servers = ast.literal_eval(
            row['dns_server'])  # 使用 ast.literal_eval 安全地将字符串转换为列表
      except (ValueError, SyntaxError):
        dns_servers = [row['dns_server']]  # 如果转换失败，则将其视为单个 DNS 服务器
      for dns_server in dns_servers:
        document = {
            'timestamp': row['timestamp'],
            'domain': row['domain'],
            'dns_server': dns_server,
            'record_type': row['record_type'],
            'ips': row['answers'],
            'error_code': row['error_code'],
            'error_reason': row['error_reason']
        }
        data_dict[(row['domain'], dns_server)].append(document)

    # 逐个域名和 DNS 服务器插入多个文档到 MongoDB
    for (domain, dns_server), documents in tqdm(
        data_dict.items(),
        desc=f'Inserting data from {os.path.basename(file)}'):
      for doc in documents:
        mongodbOP_CM_DNSP.insert_one(doc)


def dump_to_mongo():
  mongodbOP_CM_DNSP = MongoDBHandler(CM_DNSP_ADC_NOV)

  if os.name == 'nt':
    FileFolderLocation = 'E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\Data-2024-11\\ChinaMobile'
  else:
    FileFolderLocation = '/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile'
  csv_files = [os.path.join(FileFolderLocation, file) for file in os.listdir(FileFolderLocation) if file.endswith('.csv')]

  # Drop the collection before inserting new data
  logger.info('Dropping the collection before inserting new data')
  CM_DNSP_ADC_NOV.drop()

  # Create an index for the domain, dns_server, and timestamp fields
  logger.info('Creating index for the domain, dns_server, and timestamp fields')
  CM_DNSP_ADC_NOV.create_index([('domain', 1), ('dns_server', 1), ('timestamp', 1)], unique=False)  # 创建包含timestamp的复合唯一索引

  with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_file, file, mongodbOP_CM_DNSP) for file in csv_files]
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc='Processing files'):
      try:
        future.result()
      except Exception as e:
        logger.error(f'Error processing file: {e}')

if __name__ == '__main__':
  dump_to_mongo()
