import concurrent.futures
import csv
import logging
import os
import os.path

from DBOperations import ADC_db, MongoDBHandler

CM_DNSP_ADC_NOV = ADC_db['ChinaMobile-DNSPoisoning-November']

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
    for row in reader:
      timestamp = row['timestamp'],
      domain = row['domain'],
      dns_server = row['dns_server'],
      record_type = row['record_type'],
      answers = row['answers'],
      error_code = row['error_code'],
      error_reason = row['error_reason']
      data = {
        'timestamp': timestamp,
        'domain': domain,
        'dns_server': dns_server,
        'record_type': record_type,
        'answers': answers,
        'error_code': error_code,
        'error_reason': error_reason
      }
      logger.info(f'Inserting DNSPoisoning data into mongodb with domain: {domain}')
      mongodbOP_CM_DNSP.find_one_and_update(data)

def dump_to_mongo():
  mongodbOP_CM_DNSP = MongoDBHandler(CM_DNSP_ADC_NOV)
  FileFolderLocation = '/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile'
  csv_files = [os.path.join(FileFolderLocation, file) for file in os.listdir(FileFolderLocation) if file.endswith('.csv')]

  with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_file, file, mongodbOP_CM_DNSP) for file in csv_files]
    for future in concurrent.futures.as_completed(futures):
      try:
        future.result()
      except Exception as e:
        logger.error(f'Error processing file: {e}')

if __name__ == '__main__':
  dump_to_mongo()
