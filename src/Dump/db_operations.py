import logging

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Set up the logger
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
# Connect to local mongodb server and connect to the BeforeDomainChange database
try:
  client = MongoClient('localhost', 27017)
  BDC_db = client.BeforeDomainChange
  ADC_db = client.AfterDomainChange
except ConnectionFailure as e:
  logger.error(f"Could not connect to the server: {e}")
except OperationFailure as e:
  logger.error(f"Could not connect to the server: {e}")
#connect to collection

class MongoDBHandler:
  def __init__(self, collection):
    self.collection = collection

  def find_one_and_update(self, data: dict) -> None:
    if self.collection.find_one(data):
      self.collection.update_one(data, {"$set": data})
    else:
      self.collection.insert_one(data)


  def delete_many(self, data: dict) -> None:
    self.collection.delete_many(data)

  def lookup(self, data: dict) -> bool:
    if self.collection.find_one(data):
      return True
    return False

  def insert_one(self, data: dict) -> None:
    self.collection.insert_one(data)


  def delete_one(self, data: dict) -> None:
    self.collection.delete_one(data)


  def update_one(self, data: dict) -> None:
    self.collection.update_one(data, {"$set": data})

  def aggregate(self, pipeline: list) -> list:
    return self.collection.aggregate(pipeline)

  def find(self, data: dict) -> list:
    return self.collection.find(data)
