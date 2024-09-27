import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from concurrent.futures import ThreadPoolExecutor

from requests import delete

# Set up the logger
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
# Connect to local mongodb server and connect to the BeforeDocmainChange database
try:
  client = MongoClient('localhost', 27017)
  BDC_db = client.BeforeDomainChange
  ADC_db = client.AfterDomainChange
except ConnectionFailure as e:
  logger.error(f"Could not connect to the server: {e}")
except OperationFailure as e:
  logger.error(f"Could not connect to the server: {e}")
#connect to collection


CT_DNSP_ADC = ADC_db['China-Telecom-DNSPoisoning']
CT_GFWL_ADC = ADC_db['China-Telecom-GFWLocation']
CT_IPB_ADC = ADC_db['China-Telecom-IPBlocking']


AfterDomainChangeFolder = '../Data/AfterDomainChange/'

class MongoDBHandler:
  def __init__(self, collection):
    self.collection = collection

  def find_one_and_update(self, data: dict) -> None:
    if self.collection.find_one(data):
      self.collection.update_one(data, {"$set": data})
      logger.info("Updated the data in the collection")
    else:
      self.collection.insert_one(data)
      logger.info("Inserted the data into the collection")

  def delete_many(self, data: dict) -> None:
    self.collection.delete_many(data)
    logger.info("Deleted the data from the collection")

  def lookup(self, data: dict) -> bool:
    if self.collection.find_one(data):
      return True
    return False

  def insert_one(self, data: dict) -> None:
    self.collection.insert_one(data)
    logger.info("Inserted the data into the collection")

  def delete_one(self, data: dict) -> None:
    self.collection.delete_one(data)
    logger.info("Deleted the data from the collection")

  def update_one(self, data: dict) -> None:
    self.collection.update_one(data, {"$set": data})
    logger.info("Updated the data in the collection")
