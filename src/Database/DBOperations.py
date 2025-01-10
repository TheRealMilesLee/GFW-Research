import logging

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Set up the logger
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
# Connect to local mongodb server and connect to the BeforeDomainChange database
try:
  client = MongoClient('mongodb://localhost:27017', maxPoolSize=65535)
  BDC_db = client.BeforeDomainChange
  ADC_db = client.AfterDomainChange
  Merged_db = client.MergedDatabase
  CompareGroup_db = client.CompareGroup
except ConnectionFailure as e:
  logger.error(f"Could not connect to the server: {e}")
except OperationFailure as e:
  logger.error(f"Could not connect to the server: {e}")
#connect to collection

class MongoDBHandler:
    def __init__(self, collection):
        self.collection = collection

    def find_one_and_update(self, query: dict, update_data: dict, upsert: bool) -> None:
        """
        Find one document and update it with the specified update data.
        `update_data` should include MongoDB operators like $set or $addToSet.
        """
        self.collection.find_one_and_update(query, update_data, upsert=upsert)

    def delete_many(self, data: dict) -> None:
        self.collection.delete_many(data)

    def lookup(self, data: dict) -> bool:
        return bool(self.collection.find_one(data))

    def insert_one(self, data: dict) -> None:
        self.collection.insert_one(data)

    def delete_one(self, data: dict) -> None:
        self.collection.delete_one(data)

    def update_one(self, query: dict, update_data: dict, upsert: bool) -> None:
        """
        Update one document with the specified update data.
        `update_data` should include MongoDB operators like $set or $addToSet.
        """
        self.collection.update_one(query, update_data, upsert=upsert)

    def aggregate(self, pipeline: list) -> list:
        return list(self.collection.aggregate(pipeline))

    def find(self, data: dict) -> list:
        return list(self.collection.find(data))

    def insert_many(self, data: list) -> None:
        self.collection.insert_many(data)

    def find_one(self, data: dict, projection: dict = None) -> dict:
        return self.collection.find_one(data, projection)

    def count_documents(self, data: dict) -> int:
        return self.collection.count_documents(data)

    def create_index(self, index: str, unique: bool) -> None:
        self.collection.create_index(index, unique=unique)

    def drop(self) -> None:
        self.collection.drop()  # Drop the collection

    def get_all_documents(self) -> list:
        return list(self.collection.find())

    def distinct(self, field: str) -> list:
        return self.collection.distinct(field)

    def getAllDocuments(self):
        return self.collection.find()


