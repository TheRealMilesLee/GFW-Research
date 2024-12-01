import csv
import logging
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor

from DBOperations import BDC_db, MongoDBHandler
from tqdm import tqdm

# Constants
if os.name == 'nt':
  BeforeDomainChangeFolder = 'E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\BeforeDomainChange'
else:
  BeforeDomainChangeFolder = '/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/BeforeDomainChange/'
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 64)  # Dynamically set workers

# 移除所有现有的处理程序
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
# 创建 logger
logger = logging.getLogger(__name__)
def toBoolean(value: str) -> bool:
  return value == 'True'

class DataProcessor:
  def __init__(self, db_handler, folder_location):
    self.db_handler = db_handler
    self.folder_location = folder_location

  def process(self):
    raise NotImplementedError

  def merge_results(self, readingResults, unique_key):
    merged_results = {}
    for result in readingResults:
      key = result[unique_key]
      if key not in merged_results:
        merged_results[key] = {unique_key: key}
        for k in result:
          if k != unique_key:
            merged_results[key][k] = []
      for k in result:
        if k != unique_key and result[k] not in merged_results[key][k]:
          merged_results[key][k].append(result[k])
    return list(merged_results.values())

class CSVProcessor(DataProcessor):
  def process_csv(self, file):
    readingResults = []
    with open(os.path.join(self.folder_location, file), 'r', encoding='utf-8') as csv_file:
      csv_reader = csv.reader(csv_file)
      for row in csv_reader:
        if row[0] == 'timestamp':
          continue
        readingResults.append(self.format_row(row))
    return readingResults

  def format_row(self, row):
    raise NotImplementedError

class TextProcessor(DataProcessor):
  def process_txt(self, file):
    readingResults = []
    with open(os.path.join(self.folder_location, file), 'r', encoding='utf-8') as file:
      lines = file.read().strip().split('\n')
      for line in lines:
        readingResults.append(self.format_line(line))
    return readingResults

  def format_line(self, line):
    raise NotImplementedError

class CM_DNSP_Processor(CSVProcessor):
  def process(self):
    self.db_handler.drop()
    readingResults = []
    for file in os.listdir(self.folder_location):
      if file.endswith('.csv'):
        readingResults.extend(self.process_csv(file))
    readingResults = self.merge_results(readingResults, 'domain')
    self.db_handler.create_index('domain', unique=True)
    return readingResults

  def format_row(self, row):
    determined_poisoned = toBoolean(row[7]) and toBoolean(row[8])
    return {
      'timestamp': row[0],
      'domain': row[1],
      'results': ','.join([r for r in row[2:6] if len(r) > 7]),
      'is_poisoned': determined_poisoned
    }

class CM_GFWL_Processor(TextProcessor):
  def process(self):
    self.db_handler.drop()
    readingResults = []
    for file in os.listdir(self.folder_location):
      if file.endswith('.txt'):
        readingResults.extend(self.process_txt(file))
    readingResults = self.merge_results(readingResults, 'domain')
    self.db_handler.create_index('domain', unique=True)
    return readingResults

  def format_line(self, line):
    domain, result = line.split(':')
    result = result.strip()
    if result.startswith('Possible GFW detection'):
      result = result.split('(')[1].strip(')')
    elif result.startswith('No GFW detection'):
      result = "No GFW detection"
    else:
      result = "Traceroute Failed"
    return {"domain": domain, "result": result}

class CM_IPB_Processor(CSVProcessor, TextProcessor):
  def process(self):
    self.db_handler.drop()
    readingResults = []
    for file in os.listdir(self.folder_location):
      if file.endswith('.csv'):
        readingResults.extend(self.process_csv(file))
      elif file.endswith('.txt'):
        readingResults.extend(self.process_txt(file))
    readingResults = self.merge_results(readingResults, 'domain')
    self.db_handler.create_index('domain', unique=True)
    return readingResults

  def format_row(self, row):
    return {
      "timestamp": row[0],
      "domain": row[1],
      "results_ip": row[2],
      "ip_type": row[3],
      "port": row[4],
      "is_accessible": row[5]
    }

  def format_line(self, line):
    domain, result = line.split(':')
    result = result.strip()
    if result.startswith('No GFW detected'):
      result = result.split('(')[1].strip(')')
    else:
      result = "Not Found"
    return {"domain": domain, "result": result}

class CT_IPB_Processor(CSVProcessor):
  def process(self):
    self.db_handler.drop()
    readingResults = []
    for file in os.listdir(self.folder_location):
      if file.endswith('.csv'):
        readingResults.extend(self.process_csv(file))
    readingResults = self.merge_results(readingResults, 'domain')
    for result in readingResults:
      if 'is_accessible' in result:
        if 'True' in result['is_accessible'] and 'False' in result['is_accessible']:
          result['is_accessible'] = 'sometimes'
        elif 'True' in result['is_accessible']:
          result['is_accessible'] = 'True'
        else:
          result['is_accessible'] = 'False'
    self.db_handler.create_index('domain', unique=True)
    return readingResults

  def format_row(self, row):
    return {
      "timestamp": row[0],
      "domain": row[1],
      "results_ip": row[2],
      "ip_type": row[3],
      "port": row[4],
      "is_accessible": row[5]
    }

class UCD_DNSP_Processor(CM_DNSP_Processor):
  pass

class UCD_GFWL_Processor(CM_GFWL_Processor):
  pass

class UCD_IPB_Processor(CM_IPB_Processor):
  pass

class insert_into_db:
  def __init__(self, db_handler, data):
    self.db_handler = db_handler
    self.data = data

  def insert(self):
    self.db_handler.insert_many(self.data)

# Example usage
if __name__ == "__main__":
  logger.info("Databases cleaned up")
  processors = [
    CM_DNSP_Processor(MongoDBHandler(BDC_db['China-Mobile-DNSPoisoning']), os.path.join(BeforeDomainChangeFolder, 'DNSPoisoning/')),
    CM_GFWL_Processor(MongoDBHandler(BDC_db['China-Mobile-GFWLocation']), os.path.join(BeforeDomainChangeFolder, 'GFWLocation/')),
    CM_IPB_Processor(MongoDBHandler(BDC_db['China-Mobile-IPBlocking']), os.path.join(BeforeDomainChangeFolder, 'IPBlocking/')),
    CT_IPB_Processor(MongoDBHandler(BDC_db['China-Telecom-IPBlocking']), os.path.join(BeforeDomainChangeFolder, 'Mac', 'IPBlocking')),
    UCD_DNSP_Processor(MongoDBHandler(BDC_db['UCDavis-CompareGroup-DNSPoisoning']), os.path.join(BeforeDomainChangeFolder, 'CompareGroup', 'DNSPoisoning/')),
    UCD_GFWL_Processor(MongoDBHandler(BDC_db['UCDavis-CompareGroup-GFWLocation']), os.path.join(BeforeDomainChangeFolder, 'CompareGroup', 'GFWLocation/')),
    UCD_IPB_Processor(MongoDBHandler(BDC_db['UCDavis-CompareGroup-IPBlocking']), os.path.join(BeforeDomainChangeFolder, 'CompareGroup', 'IPBlocking/'))
  ]
  def process_processor(processor):
    results = processor.process()
    insert_into_db(processor.db_handler, results).insert()
    logger.info(f"Processed {len(results)} records for {processor.__class__.__name__}")

  with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    executor.map(process_processor, processors)
