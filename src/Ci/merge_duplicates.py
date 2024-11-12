import logging
from Dump.db_operations import ADC_db, BDC_db, MongoDBHandler

# 移除所有现有的处理程序
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
# 创建 logger
logger = logging.getLogger(__name__)


# After Domain Change Database
CM_DNSP_ADC = ADC_db["China-Mobile-DNSPoisoning"]
CM_GFWL_ADC = ADC_db["China-Mobile-GFWLocation"]
CT_DNSP_ADC = ADC_db["China-Telecom-DNSPoisoning"]
CT_GFWL_ADC = ADC_db["China-Telecom-GFWLocation"]
CT_IPB_ADC = ADC_db["China-Telecom-IPBlocking"]
UCD_GFWL_ADC = ADC_db["UCDavis-Server-GFWLocation"]
UCD_IPB_ADC = ADC_db["UCDavis-Server-IPBlocking"]
UCD_DNSP_ADC = ADC_db["UCDavis-Server-DNSPoisoning"]

# Before Domain Change Database
CM_DNSP = BDC_db["China-Mobile-DNSPoisoning"]
CM_GFWL = BDC_db["China-Mobile-GFWLocation"]
CM_IPB = BDC_db["China-Mobile-IPBlocking"]
# China Telecom Database
CT_IPB = BDC_db["China-Telecom-IPBlocking"]
# UCD Compare Group
UCD_CG_DNSP = BDC_db["UCDavis-CompareGroup-DNSPoisoning"]
UCD_CG_GFWL = BDC_db["UCDavis-CompareGroup-GFWLocation"]
UCD_CG_IPB = BDC_db["UCDavis-CompareGroup-IPBlocking"]


class DomainCleaner:
    def __init__(self, db_handler, collection, logger):
        self.db_handler = db_handler
        self.collection = collection
        self.logger = logger

    def clean(self):
        self.logger.info(f"Cleaning up {self.collection.name}")
        mongodb_handler = MongoDBHandler(self.collection)
        pipeline = [
            {
                "$group": {
                    "_id": "$domain",
                    "count": {"$sum": 1},
                    "document_ids": {"$push": "$_id"},
                }
            },
            {"$match": {"count": {"$gt": 1}}},
        ]
        try:
            domains_results = list(mongodb_handler.aggregate(pipeline))
            self.logger.info(f"Found {len(domains_results)} duplicate domains")
            for domain_info in domains_results:
                domain = domain_info["_id"]
                count = domain_info["count"]
                self.logger.info(f"Domain: {domain} (appeared {count} times)")
                documents = mongodb_handler.find({"domain": domain})
                aggregated_doc = self.aggregate_documents(domain, documents)
                mongodb_handler.delete_many({"domain": domain})
                mongodb_handler.insert_one(aggregated_doc)
                self.logger.info(f"Merged {count} documents for domain {domain}")
        except Exception as e:
            self.logger.error(f"Error occurred: {str(e)}")

    def aggregate_documents(self, domain, documents):
        aggregated_doc = {"domain": domain, "results": []}
        for doc in documents:
            result = self.extract_result(doc)
            aggregated_doc["results"].append(result)
        return aggregated_doc

    def extract_result(self, doc):
        raise NotImplementedError("Subclasses should implement this method")


class DNSPoisoningCleaner(DomainCleaner):
    def extract_result(self, doc):
        return {
            "timestamp": doc["timestamp"],
            "china_result_ipv4": doc.get("china_result_ipv4"),
            "china_result_ipv6": doc.get("china_result_ipv6"),
            "global_result_ipv4": doc.get("global_result_ipv4"),
            "global_result_ipv6": doc.get("global_result_ipv6"),
            "is_poisoned": doc.get("is_poisoned"),
            "is_poisoned_IPv4": doc.get("is_poisoned_IPv4"),
            "is_poisoned_ipv6": doc.get("is_poisoned_ipv6"),
        }


class GFWLocationCleaner(DomainCleaner):
    def extract_result(self, doc):
        return {
            "timestamp": doc["timestamp"],
            "gfw_detected": doc.get("gfw_detected"),
            "reached_destination": doc.get("reached_destination"),
        }


class IPBlockingCleaner(DomainCleaner):
    def extract_result(self, doc):
        return {
            "timestamp": doc["timestamp"],
            "ip": doc.get("ip"),
            "ip_type": doc.get("ip_type"),
            "port": doc.get("port"),
            "is_accessible": doc.get("is_accessible"),
        }

class DNSPoisoningCleaner_ADC(DomainCleaner):
  def extract_result(self, doc):
    return {
      "timestamp": doc['timestamp'],
      "dns_server": doc['dns_server'],
      "result_ipv4": doc['result_ipv4'],
      "result_ipv6": doc['result_ipv6'],
    }

class GFWLocationCleaner_ADC(DomainCleaner):
  def extract_result(self, doc):
    return {
      "timestamp": doc['timestamp'],
      "ips": doc['ips'],
      "location": doc['location'],
    }

def cleanUP_BeforeDomainChange():
  cleaners = [
    DNSPoisoningCleaner(MongoDBHandler, CM_DNSP, logger),
    GFWLocationCleaner(MongoDBHandler, CM_GFWL, logger),
    IPBlockingCleaner(MongoDBHandler, CM_IPB, logger),
    IPBlockingCleaner(MongoDBHandler, CT_IPB, logger),
    DNSPoisoningCleaner(MongoDBHandler, UCD_CG_DNSP, logger),
    GFWLocationCleaner(MongoDBHandler, UCD_CG_GFWL, logger),
    IPBlockingCleaner(MongoDBHandler, UCD_CG_IPB, logger)
  ]

  for cleaner in cleaners:
      cleaner.clean()

def cleanUP_AfterDomainChange():
  cleaners = [
    DNSPoisoningCleaner_ADC(MongoDBHandler, CM_DNSP_ADC, logger),
    GFWLocationCleaner_ADC(MongoDBHandler, CM_GFWL_ADC, logger),
    DNSPoisoningCleaner(MongoDBHandler, CT_DNSP_ADC, logger),
    GFWLocationCleaner(MongoDBHandler, CT_GFWL_ADC, logger),
    IPBlockingCleaner(MongoDBHandler, CT_IPB_ADC, logger),
    GFWLocationCleaner(MongoDBHandler, UCD_GFWL_ADC, logger),
    IPBlockingCleaner(MongoDBHandler, UCD_IPB_ADC, logger),
    DNSPoisoningCleaner(MongoDBHandler, UCD_DNSP_ADC, logger)
  ]

  for cleaner in cleaners:
    cleaner.clean()

if __name__ == '__main__':
  # cleanUP_BeforeDomainChange()
  cleanUP_AfterDomainChange()
