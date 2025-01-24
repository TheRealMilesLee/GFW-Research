import csv
import logging
import multiprocessing
import os
import tqdm
import ast
import re
from ..DBOperations import BDC_db, MongoDBHandler
import hashlib

# Constants
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 64)  # Dynamically set workers

CM_DNSP_BDC = MongoDBHandler(BDC_db['China-Mobile-DNSPoisoning'])
UCD_DNSP_BDC = MongoDBHandler(BDC_db['UCDavis-CompareGroup-DNSPoisoning'])
# 移除所有现有的处理程序
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
# 创建 logger
logger = logging.getLogger(__name__)
def toBoolean(value: str) -> bool:
  return value == 'True'

# DNS Poisoning results
def BDC_DNSP_Dump(db, folder_location: str) -> list:
  db.drop()  # Clear the collection before inserting new data
  file_list = os.listdir(folder_location)
  counter = 0
  for file_name in tqdm.tqdm(file_list, desc=f'Processing {db.collection.name} '):
    if file_name.endswith('.csv'):
      with open(os.path.join(folder_location, file_name), 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        batch_results = []
        for row in csv_reader:
          if row[0] == 'timestamp':
            continue
          try:
            timestamp = row[0]
            domain = row[1]
            ipv4_results = set(filter(lambda ip: re.match(r'^\d{1,3}(\.\d{1,3}){3}$', ip), ast.literal_eval(row[2]) + ast.literal_eval(row[4])))
            ipv6_results = set(filter(lambda ip: len(ip) > 2 and re.match(r'^[0-9a-fA-F:]+$', ip), ast.literal_eval(row[3]) + ast.literal_eval(row[5])))

            result = {
              "_id": f"{db.collection.name}-{domain}-{timestamp}-{counter}",
              'timestamp': timestamp,
              'domain': domain,
              'ips': list(ipv4_results.union(ipv6_results)),
            }
            batch_results.append(result)
            counter += 1
          except Exception as e:
            logger.error(f"Error processing row: {row}, error: {e}")
    if batch_results:
      db.insert_many(batch_results)
  # Create indexes
  db.create_index([('domain', 1), ('timestamp', 1)], unique=False)

if __name__ == '__main__':
  BDC_DNSP_Dump(CM_DNSP_BDC, "E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\BeforeDomainChange\\DNSPoisoning")
  BDC_DNSP_Dump(UCD_DNSP_BDC, "E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\BeforeDomainChange\\CompareGroup\\DNSPoisoning")
