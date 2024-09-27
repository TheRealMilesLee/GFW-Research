from ast import Compare
import csv
import logging
import os
import os.path
import re
from concurrent.futures import ThreadPoolExecutor
import time
from dump_to_mongo_before_domain_change import CM_DNSP, FileProcessingHandler
from Helper.db_operations import ADC_db, MongoDBHandler

# Constants
CM_DNSP_ADC = ADC_db['China-Mobile-DNSPoisoning']
CT_DNSP_ADC = ADC_db['China-Telecom-DNSPoisoning']
CT_GFWL_ADC = ADC_db['China-Telecom-GFWLocation']
CT_IPB_ADC = ADC_db['China-Telecom-IPBlocking']
AfterDomainChangeFolder = '../Data/AfterDomainChange/'
ipv4_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
ipv6_pattern = r'([0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){7})'
# Set up the logger
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class DumpingData:
  def GFWLocation_ADC_dump_CT(self, folder:str, collection:str, compareGroup:bool) -> None:
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
              mongodbOP_GFWL_ADC.insert_one(data)
  def extract_timestamp(self, filename:str) -> str:
    return filename.split('_')[0]

  def DNSPoisoning_ADC_dump_CM(self, folder:str, collection:str) -> None:
    mongodbOP_DNSP_ADC = MongoDBHandler(collection)
    FileFolderLocation = folder + 'China-Mobile/DNSPoisoning/'
    for file in os.listdir(FileFolderLocation):
      if file.endswith('.csv'):
        with open(os.path.join(FileFolderLocation, file), 'r') as csvfile:
          logger.info(f'Processing file: {file}')
          reader = csv.DictReader(csvfile)
          for row in reader:
            domain = row['domain']
            result = row.get('result')
            ips = row.get('ips')
            error = row.get('error')
            gfw_blocked = False
            destination_ip = None
            if result:
              if 'No GFW detected' in result:
                match = re.search(r'Reached destination (\d{1,3}(?:\.\d{1,3}){3})', result)
              if match:
                destination_ip = match.group(1)
              if destination_ip == '127.0.0.1':
                gfw_blocked = True
              else:
                gfw_blocked = False
            elif 'No IP addresses found in traceroute' in result:
              gfw_blocked = True
            elif ips:
              ipv4_addresses = ips.get('ipv4', [])
              ipv6_addresses = ips.get('ipv6', [])
              if '127.0.0.1' in ipv4_addresses:
                gfw_blocked = True
                destination_ip = ipv4_addresses[0] if ipv4_addresses else None
            elif error:
              gfw_blocked = True
            data = {
              'domain': domain,
              'gfw_blocked': gfw_blocked,
              'destination_ip': destination_ip,
              'error': error,
              'ips': {
              'ipv4': ipv4_addresses,
              'ipv6': ipv6_addresses
              }
            }
          mongodbOP_DNSP_ADC.insert_one(data)

  def IPBlocking_ADC_dump(self, folder:str, collection:str, CT:bool, compareGroup:bool) -> None:
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
            mongodbOP_IPB_ADC.insert_one(data)

if __name__ == '__main__':
  dump = DumpingData()
  CT_Dump = FileProcessingHandler()
  with ThreadPoolExecutor() as executor:
    futures = [
      executor.submit(dump.GFWLocation_ADC_dump_CT, AfterDomainChangeFolder, CT_GFWL_ADC, False),
      executor.submit(dump.DNSPoisoning_ADC_dump_CM, AfterDomainChangeFolder, CM_DNSP_ADC),
      executor.submit(CT_Dump.DNSPoisoning_dump, f'{AfterDomainChangeFolder}/China-Telecom', CT_DNSP_ADC, False),
      executor.submit(dump.IPBlocking_ADC_dump, AfterDomainChangeFolder, CT_IPB_ADC, True, False),
      executor.submit(dump.IPBlocking_ADC_dump, AfterDomainChangeFolder, CT_IPB_ADC, False, False),
      executor.submit(dump.GFWLocation_ADC_dump_CT, AfterDomainChangeFolder, CT_GFWL_ADC, True),
      executor.submit(dump.IPBlocking_ADC_dump, AfterDomainChangeFolder, CT_IPB_ADC, False, True),
      executor.submit(dump.IPBlocking_ADC_dump, AfterDomainChangeFolder, CT_IPB_ADC, True, True)
    ]
    for future in futures:
      future.result()  # Wait for all futures to complete
  logger.info('Data dump to MongoDB completed')
