import matplotlib.pyplot as plt

from ..src.Database.DBOperations import CompareGroup_db, Merged_db, MongoDBHandler

# Merged_db constants
DNSPoisoning = MongoDBHandler(Merged_db["DNSPoisoning"])

# CompareGroup_db constants
DNSPoisoningCompareGroup = MongoDBHandler(CompareGroup_db["DNSPoisoning"])

def MergeDB_DNSPoisoning_Plot():
  data = DNSPoisoning.get_all_documents()
