import logging

from sympy import Q

from Dump.db_operations import ADC_db, BDC_db, MongoDBHandler

# 移除所有现有的处理程序
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 创建 logger
logger = logging.getLogger(__name__)


# After Domain Change Database
CM_DNSP_ADC = ADC_db['China-Mobile-DNSPoisoning']
CM_GFWL_ADC = ADC_db['China-Mobile-GFWLocation']
CT_DNSP_ADC = ADC_db['China-Telecom-DNSPoisoning']
CT_GFWL_ADC = ADC_db['China-Telecom-GFWLocation']
CT_IPB_ADC = ADC_db['China-Telecom-IPBlocking']
UCD_GFWL_ADC = ADC_db['UCDavis-Server-GFWLocation']
UCD_IPB_ADC = ADC_db['UCDavis-Server-IPBlocking']
UCD_DNSP_ADC = ADC_db['UCDavis-Server-DNSPoisoning']

# Before Domain Change Database
CM_DNSP = BDC_db['China-Mobile-DNSPoisoning']
CM_GFWL = BDC_db['China-Mobile-GFWLocation']
CM_IPB = BDC_db['China-Mobile-IPBlocking']
# China Telecom Database
CT_IPB = BDC_db['China-Telecom-IPBlocking']
# UCD Compare Group
UCD_CG_DNSP = BDC_db['UCDavis-CompareGroup-DNSPoisoning']
UCD_CG_GFWL = BDC_db['UCDavis-CompareGroup-GFWLocation']
UCD_CG_IPB = BDC_db['UCDavis-CompareGroup-IPBlocking']


def cleanUP_BeforeDomainChange():
  # Start with China-Mobile DNSPoisoning. First, gather all documents with the same domain
  # and then merge them into one document
  logger.info('Cleaning up China-Mobile DNSPoisoning')
  mongodbCM_DNSP_Before = MongoDBHandler(CM_DNSP)
  pipeline = [
        # 按domain分组并计数
        {
            '$group': {
                '_id': '$domain',
                'count': {'$sum': 1},
                'document_ids': {'$push': '$_id'}  # 收集每个domain的文档ID
            }
        },
        # 筛选出现次数大于1的domain
        {
            '$match': {
                'count': {'$gt': 1}
            }
        }
    ]
  try:
    # 执行聚合查询
    domains_results = list(mongodbCM_DNSP_Before.aggregate(pipeline))
    # 打印结果
    print(f"找到 {len(domains_results)} 个重复的domains\n")
    # 对每个重复的domain查找相关文档
    for domain_info in domains_results:
      domain = domain_info['_id']
      count = domain_info['count']
      print(f"\nDomain: {domain} (出现 {count} 次)")
      # 查找具有这个domain的所有文档
      documents = mongodbCM_DNSP_Before.find({'domain': domain})
      aggregated_doc = {
        "domain": domain,
        "results": []
      }
      for doc in documents:
        timestamp = doc['timestamp']
        result = {
          "timestamp": timestamp,
          "china_result_ipv4": doc.get("china_result_ipv4"),
          "china_result_ipv6": doc.get("china_result_ipv6"),
          "global_result_ipv4": doc.get("global_result_ipv4"),
          "global_result_ipv6": doc.get("global_result_ipv6"),
          "is_poisoned": doc.get("is_poisoned"),
          "is_poisoned_IPv4": doc.get("is_poisoned_IPv4"),
          "is_poisoned_ipv6": doc.get("is_poisoned_ipv6")
        }
        aggregated_doc["results"].append(result)

      # 删除原始文档
      mongodbCM_DNSP_Before.delete_many({'domain': domain})

      # 插入聚合后的文档
      mongodbCM_DNSP_Before.insert_one(aggregated_doc)
      print(f"对于{domain}, 已合并 {count} 个文档")
  except Exception as e:
      print(f"发生错误: {str(e)}")

  # Next, clean up China-Mobile GFWLocation
  logger.info('Cleaning up China-Mobile GFWLocation')
  mongodbCM_GFWL_Before = MongoDBHandler(CM_GFWL)
  pipeline = [
        {
            '$group': {
                '_id': '$domain',
                'count': {'$sum': 1},
                'document_ids': {'$push': '$_id'}
            }
        },
        {
            '$match': {
                'count': {'$gt': 1}
            }
        }
    ]
  try:
    domains_results = list(mongodbCM_GFWL_Before.aggregate(pipeline))
    print(f"找到 {len(domains_results)} 个重复的domains\n")
    for domain_info in domains_results:
      domain = domain_info['_id']
      count = domain_info['count']
      print(f"\nDomain: {domain} (出现 {count} 次)")
      documents = mongodbCM_GFWL_Before.find({'domain': domain})
      aggregated_doc = {
        "domain": domain,
        "results": []
      }
      for doc in documents:
        timestamp = doc['timestamp']
        result = {
          "timestamp": timestamp,
          "gfw_detected": doc.get("gfw_detected"),
          "reached_destination": doc.get("reached_destination")
        }
        aggregated_doc["results"].append(result)

      mongodbCM_GFWL_Before.delete_many({'domain': domain})

      mongodbCM_GFWL_Before.insert_one(aggregated_doc)
      print(f"对于{domain}, 已合并 {count} 个文档")
  except Exception as e:
    print(f"发生错误: {str(e)}")

  # Next, clean up China-Mobile IPBlocking
  logger.info('Cleaning up China-Mobile IPBlocking')
  mongodbCM_IPB_Before = MongoDBHandler(CM_IPB)
  pipeline = [
        {
            '$group': {
                '_id': '$domain',
                'count': {'$sum': 1},
                'document_ids': {'$push': '$_id'}
            }
        },
        {
            '$match': {
                'count': {'$gt': 1}
            }
        }
    ]
  try:
    domains_results = list(mongodbCM_IPB_Before.aggregate(pipeline))
    print(f"找到 {len(domains_results)} 个重复的domains\n")
    for domain_info in domains_results:
      domain = domain_info['_id']
      count = domain_info['count']
      print(f"\nDomain: {domain} (出现 {count} 次)")
      documents = mongodbCM_IPB_Before.find({'domain': domain})
      aggregated_doc = {
        "domain": domain,
        "results": []
      }
      for doc in documents:
        timestamp = doc['timestamp']
        result = {
          "timestamp": timestamp,
          "ip": doc.get("ip"),
          "ip_type": doc.get("ip_type"),
          "port": doc.get("port"),
          "is_accessible": doc.get("is_accessible")
        }
        aggregated_doc["results"].append(result)

      mongodbCM_IPB_Before.delete_many({'domain': domain})

      mongodbCM_IPB_Before.insert_one(aggregated_doc)
      print(f"对于{domain}, 已合并 {count} 个文档")
  except Exception as e:
    print(f"发生错误: {str(e)}")


if __name__ == '__main__':
  cleanUP_BeforeDomainChange()
