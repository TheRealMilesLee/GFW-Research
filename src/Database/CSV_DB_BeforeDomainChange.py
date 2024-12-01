import csv
import logging
import multiprocessing
import os
import os.path
from concurrent.futures import ThreadPoolExecutor

from DBOperations import BDC_db, MongoDBHandler

# Constants
BeforeDomainChangeFolder = 'E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\BeforeDomainChange'
# China Mobile Database
CM_DNSP = MongoDBHandler(BDC_db['China-Mobile-DNSPoisoning'])
CM_GFWL = MongoDBHandler(BDC_db['China-Mobile-GFWLocation'])
CM_IPB = MongoDBHandler(BDC_db['China-Mobile-IPBlocking'])
# China Telecom Database
CT_IPB = MongoDBHandler(BDC_db['China-Telecom-IPBlocking'])
# UCD Compare Group
UCD_CG_DNSP = MongoDBHandler(BDC_db['UCDavis-CompareGroup-DNSPoisoning'])
UCD_CG_GFWL = MongoDBHandler(BDC_db['UCDavis-CompareGroup-GFWLocation'])
UCD_CG_IPB = MongoDBHandler(BDC_db['UCDavis-CompareGroup-IPBlocking'])
# Optimize worker count based on CPU cores
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 64)  # Dynamically set workers

# 移除所有现有的处理程序
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 创建 logger
logger = logging.getLogger(__name__)

def toBoolean(value: str) -> bool:
  return value == 'True'

def CM_DNSP(folder_location: str) -> list:
  readingResults = []
  for file in os.listdir(folder_location):
    if file.endswith('.csv'):
      with open(os.path.join(folder_location, file), 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
          if row[0] == 'timestamp':
            continue
          timestamp = row[0],
          domain= row[1],
          results = row[2] + row[3] + row[4] + row[5],
          is_poisoned = (row[7].toBoolean() & row[8].toBoolean())
          formatted_row = {

          }
          readingResults.append(formatted_row)
