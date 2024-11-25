import concurrent.futures
import csv
import logging
import os
from collections import defaultdict
from datetime import datetime

from DBOperations import ADC_db, MongoDBHandler

# Config Logger
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 创建 logger
logger = logging.getLogger(__name__)

CM_GFWL_ADC_NOV = ADC_db['ChinaMobile-GFWLocation-November']

def extract_timestamp_from_filename(filename):
  basename = os.path.basename(filename)
  date_str = basename.split('_')[-1].split('.')[0]
  return datetime.strptime(date_str, '%Y%m%d')

def processingFile(file, mongodbOP_CM_GFWL):
  timestamp = extract_timestamp_from_filename(file)
  with open(file, 'r') as csvfile:
    logger.info(f'Processing file: {os.path.basename(file)}')
    reader = csv.DictReader(csvfile)
    data_dict = defaultdict(lambda: defaultdict(list))

    # 将数据分类到字典中，准备合并
    for row in reader:
      key = row['Domain']
      data_dict[key]['IPv4'].append(row['IPv4'])
      data_dict[key]['IPv6'].append(row['IPv6'])
      data_dict[key]['RST Detected'].append(row['RST Detected'])
      data_dict[key]['Redirection Detected'].append(row['Redirection Detected'])
      data_dict[key]['Invalid IP'].append(row['Invalid IP'])
      data_dict[key]['Error'].append(row['Error'])

    # 逐个域名处理数据并更新到MongoDB
    for domain, value in data_dict.items():
      data = {
        'IPv4': list(set(value['IPv4'])),
        'IPv6': list(set(value['IPv6'])),
        'RST Detected': list(set(value['RST Detected'])),
        'Redirection Detected': list(set(value['Redirection Detected'])),
        'Invalid IP': list(set(value['Invalid IP'])),
        'Error': list(set(value['Error'])),
        'timestamp': timestamp
      }

      # 使用 $addToSet 确保数组中的唯一值
      update_data = {
        '$addToSet': {
          'IPv4': {'$each': data['IPv4']},
          'IPv6': {'$each': data['IPv6']},
          'RST Detected': {'$each': data['RST Detected']},
          'Redirection Detected': {'$each': data['Redirection Detected']},
          'Invalid IP': {'$each': data['Invalid IP']},
          'Error': {'$each': data['Error']}
        },
        '$set': {
          'timestamp': data['timestamp']
        }
      }

      logger.info(f'Inserting DNSPoisoning data into MongoDB with domain: {domain}')
      mongodbOP_CM_GFWL.update_one({'domain': domain}, update_data, upsert=True)

def dump_to_mongo():
  mongodbOP_CM_GFWL = MongoDBHandler(CM_GFWL_ADC_NOV)

  if os.name == 'nt':
    FileFolderLocation = 'E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\Data-2024-11\\ChinaMobile\\GFWLocation'
  else:
    FileFolderLocation = '/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile/GFWLocation'
  csv_files = [os.path.join(FileFolderLocation, file) for file in os.listdir(FileFolderLocation) if file.endswith('.csv')]

  # Drop the collection before inserting new data
  logger.info('Dropping the collection before inserting new data')
  CM_GFWL_ADC_NOV.drop()

  with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(processingFile, file, mongodbOP_CM_GFWL) for file in csv_files]
    for future in concurrent.futures.as_completed(futures):
      try:
        future.result()
      except Exception as e:
        logger.error(f'Error processing file: {e}')

if __name__ == '__main__':
  dump_to_mongo()
