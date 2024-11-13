"""
@brief: This script is used to dump the data from the csv file to the mongo database.
@Author: Hengyi Li
@Date: 2024-09-24
@Version: 1.0
@Copyright: (c) 2024 Hengyi Li. All rights reserved.
"""

import csv
import logging
import os
import os.path
import re
from concurrent.futures import ThreadPoolExecutor

from Database.DBOperations import BDC_db, MongoDBHandler

# Constants
BeforeDomainChangeFolder = '../../Data/BeforeDomainChange/'
# China Mobile Database
CM_DNSP = BDC_db['China-Mobile-DNSPoisoning']
CM_GFWL = BDC_db['China-Mobile-GFWLocation']
CM_IPB = BDC_db['China-Mobile-IPBlocking']
# China Telecom Database
CT_IPB = BDC_db['China-Telecom-IPBlocking']
# UCD Compare Group
UCD_CG_DNSP = BDC_db['UCDavis-CompareGroup-DNSPoisoning']
UCD_CG_GFWL = BDC_db['UCDavis-CompareGroup-GFWLocation']
UCD_CG_IPB = BDC_db['UCDavis-CompareGroup-IPBlocking']
# Regex for IPv4 and v6 pattern
ipv4_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
ipv6_pattern = r'([0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){7})'
# 移除所有现有的处理程序
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 创建 logger
logger = logging.getLogger(__name__)

class FileProcessingHandler:
  def extract_ips(self, ip_list, pattern):
    if not ip_list:
      return []
    ip_addresses = []
    for item in eval(ip_list):
      match = re.match(pattern, item)
      if match:
        ip_addresses.append(match.group())
      else:
        ip_addresses.append(None)
    return ip_addresses

  def extract_timestamp(self, filename: str) -> str:
    match = re.search(r'(\d{8}_\d{6})', filename)
    if match:
      timestamp = match.group(1)
      return f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]} {timestamp[9:11]}:{timestamp[11:13]}:{timestamp[13:15]}"
    else:
      logger.error(f"Timestamp not found in filename: {filename}")
      return None

  def parse_txt_line(self, line: str) -> tuple:
    parts = line.strip().split(':')
    domain = parts[0].strip()
    gfw_detected = 'No GFW detected' not in parts[1]
    reached_destination = 'reached destination' in parts[1]
    return domain, gfw_detected, reached_destination

  def DNSPoisoning_dump(self, folder: str, collectionName: str, compareGroup: bool) -> None:
    mongodbOP_DNS = MongoDBHandler(collectionName)
    if compareGroup:
      FileFolderLocation = folder + 'CompareGroup/DNSPoisoning/'
    else:
      FileFolderLocation = folder + 'DNSPoisoning/'

    def process_file(file):
      if file.endswith('.csv'):
        with open(os.path.join(FileFolderLocation, file), 'r') as csvfile:
          logger.info(f'Processing file: {file}')
          reader = csv.DictReader(csvfile)
          for row in reader:
            cleaned_row = {}
            for key, value in row.items():
              if value == 'None':
                cleaned_row[key] = None
              else:
                cleaned_row[key] = value
            row = cleaned_row
            cleaned_data = {
              'timestamp': row['timestamp'],
              'domain': row['domain'],
              'china_result_ipv4': self.extract_ips(row.get('china_result_ipv4'), ipv4_pattern),
              'china_result_ipv6': self.extract_ips(row.get('china_result_ipv6'), ipv6_pattern),
              'global_result_ipv4': self.extract_ips(row.get('global_result_ipv4'), ipv4_pattern),
              'global_result_ipv6': self.extract_ips(row.get('global_result_ipv6'), ipv6_pattern),
              'is_poisoned': row['is_poisoned'],
              'is_poisoned_ipv4': row['is_poisoned_ipv4'],
              'is_poisoned_ipv6': row['is_poisoned_ipv6'],
            }
            logger.info(f"Inserting DNSPoisoning data into mongodb with domain: {row['domain']}")
            mongodbOP_DNS.find_one_and_update(cleaned_data)

    with ThreadPoolExecutor() as executor:
      futures = [executor.submit(process_file, file) for file in os.listdir(FileFolderLocation)]
      for future in futures:
        try:
          future.result()
        except Exception as e:
          logger.error(f"An error occurred while processing file: {e}")


  def IPBlocking_dump(self, folder: str, collection: str, CM: bool, CompareGroup: bool) -> None:
    mongodbOP_IP = MongoDBHandler(collection)
    if CM:
      fileFolder = folder + 'IPBlocking/'
    elif not CompareGroup:
      fileFolder = folder + 'Mac/IPBlocking/'
    else:
      fileFolder = folder + 'CompareGroup/IPBlocking/'
    # Start processing files to mongodb
    for file in os.listdir(fileFolder):
      if file.endswith('.csv'):
        with open(os.path.join(fileFolder, file), 'r') as csvfile:
          logger.info(f'Processing file: {file}')
          reader = csv.DictReader(csvfile)
          for row in reader:
            row['timestamp'] = row['timestamp'].replace('T', ' ')[:19]
            logger.info(f'Inserting IPBlocking data into mongodb with domain: {row["domain"]}')
            mongodbOP_IP.find_one_and_update(row)

  def GFWLocation_dump(self, folder: str, collection: str, CompareGroup: bool) -> None:
    mongodbOP_Location = MongoDBHandler(collection)
    if CompareGroup:
      fileFolder = folder + 'CompareGroup/GFWLocation/'
    else:
      fileFolder = folder + 'GFWLocation/'
    for file in os.listdir(fileFolder):
      if file.endswith('.txt'):
        with open(os.path.join(fileFolder, file), 'r') as txtfile:
          logger.info(f'Processing file: {file}')
          timestamp = self.extract_timestamp(file)
          for line in txtfile:
            domain, gfw_detected, reached_destination = self.parse_txt_line(line)
            data = {
              'timestamp': timestamp,
              'domain': domain,
              'gfw_detected': gfw_detected,
              'reached_destination': reached_destination
            }
            logger.info(f'Inserting GFWLocation data into mongodb with domain: {domain}')
            mongodbOP_Location.find_one_and_update(data)

if __name__ == '__main__':
  processData = FileProcessingHandler()
  CM_DNSP.delete_many({})
  UCD_CG_DNSP.delete_many({})
  CM_GFWL.delete_many({})
  UCD_CG_GFWL.delete_many({})
  CM_IPB.delete_many({})
  UCD_CG_IPB.delete_many({})
  CT_IPB.delete_many({})
  logger.info('Start dumping data to the database')
  #dump the data to the collection
  with ThreadPoolExecutor() as executor:
    futures = [
      executor.submit(processData.DNSPoisoning_dump, BeforeDomainChangeFolder, CM_DNSP, False), # China Mobile DNS Poisoning Check
      executor.submit(processData.DNSPoisoning_dump, BeforeDomainChangeFolder, UCD_CG_DNSP, True), # UCD Compare Group DNS Poisoning Check
      executor.submit(processData.GFWLocation_dump, BeforeDomainChangeFolder, CM_GFWL, False),
      executor.submit(processData.GFWLocation_dump, BeforeDomainChangeFolder, UCD_CG_GFWL, True),
      executor.submit(processData.IPBlocking_dump, BeforeDomainChangeFolder, CM_IPB, True, False), # China Mobile ip blocking
      executor.submit(processData.IPBlocking_dump, BeforeDomainChangeFolder, UCD_CG_IPB, False, True), # Compare Group ip blocking
      executor.submit(processData.IPBlocking_dump, BeforeDomainChangeFolder, CT_IPB, False, False), # China Telecom ip blocking
    ]

    for future in futures:
      try:
        future.result()
      except Exception as e:
        logger.error(f"An error occurred: {e}")
