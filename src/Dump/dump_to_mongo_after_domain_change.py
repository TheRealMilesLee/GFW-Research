import ast
import concurrent.futures
import csv
import json
import logging
import os
import os.path
import re
from db_operations import ADC_db, MongoDBHandler
from dump_to_mongo_before_domain_change import FileProcessingHandler

# Constants
CM_DNSP_ADC = ADC_db['China-Mobile-DNSPoisoning-IPBlocking']
CM_GFWL_ADC = ADC_db['China-Mobile-GFWLocation']

CT_DNSP_ADC = ADC_db['China-Telecom-DNSPoisoning']
CT_GFWL_ADC = ADC_db['China-Telecom-GFWLocation']
CT_IPB_ADC = ADC_db['China-Telecom-IPBlocking']

UCD_GFWL_ADC = ADC_db['UCDavis-Server-GFWLocation']
UCD_IPB_ADC = ADC_db['UCDavis-Server-IPBlocking']
UCD_DNSP_ADC = ADC_db['UCDavis-Server-DNSPoisoning']

AfterDomainChangeFolder = '../../Data/AfterDomainChange/'
ipv4_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
ipv6_pattern = r'([0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){7})'

# 移除所有现有的处理程序
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 创建 logger
logger = logging.getLogger(__name__)

class DumpingData:
  def GFWLocation_ADC_dump_CT(self, folder: str, collection: str, compareGroup: bool) -> None:
    mongodbOP_GFWL_ADC = MongoDBHandler(collection)
    if not compareGroup:
      FileFolderLocation = folder + 'China-Telecom/GFWDeployed/'
    else:
      FileFolderLocation = folder + 'UCDavis-Server/GFWLocation/'
    for file in os.listdir(FileFolderLocation):
      if file.endswith('.txt'):
        with open(os.path.join(FileFolderLocation, file), 'r') as txtfile:
          logger.info(f'Processing file: {file}')
          timestamp = self.extract_timestamp(file)
          for line in txtfile:
            domain, result = line.split(': ', 1)
            gfw_blocked = False
            destination_ip = None
            if 'No IP addresses found in traceroute' in result:
              gfw_blocked = True
            elif 'No GFW detected' in result:
              match = re.search(r'Reached destination (\d{1,3}(?:\.\d{1,3}){3})', result)
              if match:
                destination_ip = match.group(1)
              if destination_ip == '127.0.0.1':
                gfw_blocked = True
              else:
                gfw_blocked = False
              data = {
                'timestamp': timestamp,
                'domain': domain,
                'gfw_blocked': gfw_blocked,
                'destination_ip': destination_ip
              }
              logger.info(f'Inserting GFWLocation data into mongodb with domain: {domain}')
              mongodbOP_GFWL_ADC.insert_one(data)

  def extract_timestamp(self, filename: str) -> str:
    return filename.split('_')[0]

  def GFWLocation_ADC_dump_CM(self, folder: str, collection: str) -> None:
    mongodbOP_DNSP_ADC = MongoDBHandler(collection)
    FileFolderLocation = folder + 'China-Mobile/GFWLocation/'
    for file in os.listdir(FileFolderLocation):
      if file.endswith('.csv'):
        with open(os.path.join(FileFolderLocation, file), 'r') as csvfile:
          logger.info(f'Processing file: {file}')
          for line in csvfile:
            try:
              data = ast.literal_eval(line)
              logger.info(f'Inserting GFWLocation into mongodb with domain: {data["domain"]}')
              mongodbOP_DNSP_ADC.insert_one(data)
            except json.JSONDecodeError as e:
              print(f"Error parsing line: {e}")

  def IPBlocking_ADC_dump(self, folder: str, collection: str, CT: bool, compareGroup: bool) -> None:
    mongodbOP_IPB_ADC = MongoDBHandler(collection)
    if CT:
      FileFolderLocation = folder + 'China-Telecom/IPBlocking/'
    elif not compareGroup:
      FileFolderLocation = folder + 'China-Mobile/IPBlocking/'
    else:
      FileFolderLocation = folder + 'UCDavis-Server/IPBlocking/'
    for file in os.listdir(FileFolderLocation):
      if file.endswith('.csv'):
        with open(os.path.join(FileFolderLocation, file), 'r') as csvfile:
          logger.info(f'Processing file: {file}')
          reader = csv.DictReader(csvfile)
          for row in reader:
            timestamp = row['timestamp']
            domain = row['domain']
            ip = row['ip']
            ip_type = row['ip_type']
            port = row['port']
            is_accessible = row['is_accessible']
            data = {
              'timestamp': timestamp,
              'domain': domain,
              'ip': ip,
              'ip_type': ip_type,
              'port': port,
              'is_accessible': is_accessible
            }
            logger.info(f'Inserting IPBlocking data into mongodb with domain: {domain}')
            mongodbOP_IPB_ADC.insert_one(data)

def main():
  dump = DumpingData()
  CT_Dump = FileProcessingHandler()
  CM_DNSP_ADC.delete_many({})
  CM_GFWL_ADC.delete_many({})
  CT_DNSP_ADC.delete_many({})
  CT_GFWL_ADC.delete_many({})
  CT_IPB_ADC.delete_many({})
  UCD_GFWL_ADC.delete_many({})
  UCD_IPB_ADC.delete_many({})
  UCD_DNSP_ADC.delete_many({})
  logger.info('Data dump to MongoDB started')

  with concurrent.futures.ThreadPoolExecutor(max_workers=1024) as executor:
    futures = []
    logger.info('***********Dumping data GFW Location***********')
    futures.append(executor.submit(dump.GFWLocation_ADC_dump_CM, AfterDomainChangeFolder, CM_GFWL_ADC))
    futures.append(executor.submit(dump.GFWLocation_ADC_dump_CT, AfterDomainChangeFolder, CT_GFWL_ADC, False))
    futures.append(executor.submit(dump.GFWLocation_ADC_dump_CT, AfterDomainChangeFolder, UCD_GFWL_ADC, True))

    logger.info('***********Dumping data IP Blocking***********')
    futures.append(executor.submit(dump.IPBlocking_ADC_dump, AfterDomainChangeFolder, CT_IPB_ADC, True, False))
    futures.append(executor.submit(dump.IPBlocking_ADC_dump, AfterDomainChangeFolder, UCD_IPB_ADC, False, True))

    logger.info('***********Dumping data DNS Poisoning***********')
    futures.append(executor.submit(CT_Dump.DNSPoisoning_dump, f'{AfterDomainChangeFolder}/China-Telecom/', CT_DNSP_ADC, False))
    futures.append(executor.submit(CT_Dump.DNSPoisoning_dump, f'{AfterDomainChangeFolder}UCDavis-Server/', UCD_DNSP_ADC, False))
    # Wait for all threads to complete
    concurrent.futures.wait(futures)

  logger.info('Data dump to MongoDB finished')

if __name__ == '__main__':
  main()
