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
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from concurrent.futures import ThreadPoolExecutor

# Set up the logger
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
# Connect to local mongodb server and connect to the BeforeDocmainChange database
try:
  client = MongoClient('localhost', 27017)
  BDC_db = client.BeforeDomainChange
except ConnectionFailure as e:
  logger.error(f"Could not connect to the server: {e}")
except OperationFailure as e:
  logger.error(f"Could not connect to the server: {e}")
#connect to collection
CM_DNSP = BDC_db['China-Mobile-DNSPoisoning']
CM_GFWL = BDC_db['China-Mobile-GFWLocation']
CM_IPB = BDC_db['China-Mobile-IPBlocking']
CT_IPB = BDC_db['China-Telecom-IPBlocking']
UCD_CG_DNSP = BDC_db['UCDavis-CompareGroup-DNSPoisoning']
UCD_CG_GFWL = BDC_db['UCDavis-CompareGroup-GFWLocation']
UCD_CG_IPB = BDC_db['UCDavis-CompareGroup-IPBlocking']

BeforeDomainChangeFolder = '../Data/BeforeDomainChange/'
AfterDomainChangeFolder = '../Data/AfterDomainChange/'
# Define the function to dump the data to the mongo database
def find_one_and_update(collection, data) -> None:
  # search this data in the collection, if found it, update it, if not found, insert it
  if collection.find_one(data):
    collection.update_one(data, {"$set": data})
    logger.info("Update the data in the collection")
  else:
    collection.insert_one(data)
    logger.info("Insert the data to the collection")

def delete_all(collection) -> None:
  collection.delete_many({})
  logger.info("Delete all the data in the collection")

def DNSPoisoning_BeforeDomainChange(folder: str, collection: str, compareGroup: bool) -> None:
  if compareGroup:
    FileFolderLocation = folder + 'CompareGroup/DNSPoisoning/'
  else:
    FileFolderLocation = folder + 'DNSPoisoning/'
  #get all the csv files in the folder
  for file in os.listdir(FileFolderLocation):
    if file.endswith('.csv'):
      with open(FileFolderLocation+file, 'r') as csvfile:
        print('Working on file:', file)
        reader = csv.DictReader(csvfile)
        for row in reader:
          # cleanup the ipv4 and ipv6 result
          if row['china_result_ipv4'] == 'None':
            row['china_result_ipv4'] = None
          if row['china_result_ipv6'] == 'None':
            row['china_result_ipv6'] = None
          if row['global_result_ipv4'] == 'None':
            row['global_result_ipv4'] = None
          if row['global_result_ipv6'] == 'None':
            row['global_result_ipv6'] = None
          cleanuped_china_result_ipv4, cleanuped_china_result_ipv6, cleanuped_global_result_ipv4, cleanuped_global_result_ipv6 = cleanup_csv(row)
          # insert the data to the collection
          data = {
            'timestamp': row['timestamp'],
            'domain': row['domain'],
            'china_result_ipv4': cleanuped_china_result_ipv4,
            'china_result_ipv6': cleanuped_china_result_ipv6,
            'global_result_ipv4': cleanuped_global_result_ipv4,
            'global_result_ipv6': cleanuped_global_result_ipv6,
            'is_poisoned': row['is_poisoned'],
            'is_poisoned_ipv4': row['is_poisoned_ipv4'],
            'is_poisoned_ipv6': row['is_poisoned_ipv6'],
          }
          find_one_and_update(collection, data)

def cleanup_csv(row: dict) -> list:
    china_result_ipv4 = row['china_result_ipv4']
    china_result_ipv6 = row['china_result_ipv6']
    global_result_ipv4 = row['global_result_ipv4']
    global_result_ipv6 = row['global_result_ipv6']
    ipv4_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
    ipv6_pattern = r'([0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){7})'
          # For china_result_ipv4, using regex to match the ip address, delete the rest of the content, only keep the ip address
    cleanuped_china_result_ipv4 = []
    if china_result_ipv4:
      # Convert the string representation of the list to an actual list
      china_result_ipv4_list = eval(china_result_ipv4)
      for item in china_result_ipv4_list:
        # Use regex to match only valid IPv4 addresses
        match = re.match(ipv4_pattern, item)
        if match:
          cleanuped_china_result_ipv4.append(match.group())
        else:
          cleanuped_china_result_ipv4.append('None')
          # For china_result_ipv6, using regex to match the ip address, delete the rest of the content, only keep the ip address
    cleanuped_china_result_ipv6 = []
    if china_result_ipv6:
      china_result_ipv6_list = eval(china_result_ipv6)
      for item in china_result_ipv6_list:
        match = re.match(ipv6_pattern, item)
        if match:
          cleanuped_china_result_ipv6.append(match.group())
        else:
          cleanuped_china_result_ipv6.append('None')
          # For global_result_ipv4, using regex to match the ip address, delete the rest of the content, only keep the ip address
    cleanuped_global_result_ipv4 = []
    if global_result_ipv4:
      global_result_ipv4_list = eval(global_result_ipv4)
      for item in global_result_ipv4_list:
        match = re.match(ipv4_pattern, item)
        if match:
          cleanuped_global_result_ipv4.append(match.group())
        else:
          cleanuped_global_result_ipv4.append('None')
          # For global_result_ipv6, using regex to match the ip address, delete the rest of the content, only keep the ip address
    cleanuped_global_result_ipv6 = []
    if global_result_ipv6:
      global_result_ipv6_list = eval(global_result_ipv6)
      for item in global_result_ipv6_list:
        match = re.match(ipv6_pattern, item)
        if match:
          cleanuped_global_result_ipv6.append(match.group())
        else:
          cleanuped_global_result_ipv6.append('None')
    return cleanuped_china_result_ipv4,cleanuped_china_result_ipv6,cleanuped_global_result_ipv4,cleanuped_global_result_ipv6


def GFWLocation_BeforeDomainChange(folder: str, collection: str, CompareGroup: bool) -> None:
  if CompareGroup:
    UCD_GFWLocationFolder = folder + 'CompareGroup/GFWLocation/'
    fileFolder = UCD_GFWLocationFolder
  else:
    CM_GFWLocationFolder = folder + 'GFWLocation/'
    fileFolder = CM_GFWLocationFolder
  #get all the csv files in the folder
  for file in os.listdir(fileFolder):
    if file.endswith('.txt'):
      with open(fileFolder+file, 'r') as txtfile:
        print('Working on file:', file)
        # get timestamp from the file name, filename format: GFW_Location_results_20240904_163512.txt, wanted timestamp: 20240904_163512 and convert it to 2024-09-04 16:35:12
        timestamp = file.split('_')[3]
        timestamp = timestamp[:4] + '-' + timestamp[4:6] + '-' + timestamp[6:8] + ' ' + timestamp[9:11] + ':' + timestamp[11:13] + ':' + timestamp[13:15]
        for line in txtfile:
          line = line.strip()
          parts = line.split(':')
          domain = parts[0].strip()
          gfw_detected = 'No GFW detected' not in parts[1]
          reached_destination = 'reached destination' in parts[1]
          data = {
          'timestamp': timestamp,
          'domain': domain,
          'gfw_detected': gfw_detected,
          'reached_destination': reached_destination
          }
          find_one_and_update(collection, data)

def IPBlocking_BeforeDomainChange(folder: str, collection: str, CM: bool, CompareGroup: bool) -> None:
  if CM and not CompareGroup:
    CM_IPBlockingFolder = folder + 'IPBlocking/'
    fileFolder = CM_IPBlockingFolder
  elif not CM and not CompareGroup:
    CT_IPBlockingFolder = folder + 'Mac/IPBlocking/'
    fileFolder = CT_IPBlockingFolder
  elif not CM and CompareGroup:
    UCD_IPBlockingFolder = folder + 'CompareGroup/IPBlocking/'
    fileFolder = UCD_IPBlockingFolder
  #get all the csv files in the folder
  for file in os.listdir(fileFolder):
    if file.endswith('.csv'):
      with open(fileFolder+file, 'r') as csvfile:
        print('Working on file:', file)
        reader = csv.DictReader(csvfile)
        for row in reader:
          timestamp = row['timestamp']
          #convert the timestamp from 2024-08-31T23:17:12.181138 to 2024-08-31 23:17:12
          timestamp = timestamp.replace('T', ' ')
          timestamp = timestamp[:19]
          domain = row['domain']
          ip = row['ip']
          ip_type = row['ip_type']
          port = row['port']
          is_accessable = row['is_accessible']
          # insert the data to the collection
          data = {
          'timestamp': timestamp,
          'domain': domain,
          'ip': ip,
          'ip_type': ip_type,
          'port': port,
          'is_accessable': is_accessable
          }
          find_one_and_update(collection, data)

if __name__ == '__main__':
  #dump the data to the collection
  with ThreadPoolExecutor() as executor:
    futures = [
      executor.submit(DNSPoisoning_BeforeDomainChange, BeforeDomainChangeFolder, CM_DNSP, False),
      executor.submit(DNSPoisoning_BeforeDomainChange, BeforeDomainChangeFolder, UCD_CG_DNSP, True),
      executor.submit(GFWLocation_BeforeDomainChange, BeforeDomainChangeFolder, CM_GFWL, False),
      executor.submit(GFWLocation_BeforeDomainChange, BeforeDomainChangeFolder, UCD_CG_GFWL, True),
      executor.submit(IPBlocking_BeforeDomainChange, BeforeDomainChangeFolder, CM_IPB, True, False),
      executor.submit(IPBlocking_BeforeDomainChange, BeforeDomainChangeFolder, UCD_CG_IPB, False, True),
      executor.submit(IPBlocking_BeforeDomainChange, BeforeDomainChangeFolder, CT_IPB, False, False)
    ]

    for future in futures:
      try:
        future.result()
      except Exception as e:
        logger.error(f"An error occurred: {e}")
  client.close()
