import logging

from ..Utils.db_operations import ADC_db, MongoDBHandler

# 移除所有现有的处理程序
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
# 创建 logger
logger = logging.getLogger(__name__)

# Constants
CM_DNSP_ADC = ADC_db['China-Mobile-DNSPoisoning']
CM_GFWL_ADC = ADC_db['China-Mobile-GFWLocation']

CT_DNSP_ADC = ADC_db['China-Telecom-DNSPoisoning']
CT_GFWL_ADC = ADC_db['China-Telecom-GFWLocation']
CT_IPB_ADC = ADC_db['China-Telecom-IPBlocking']

UCD_GFWL_ADC = ADC_db['UCDavis-Server-GFWLocation']
UCD_IPB_ADC = ADC_db['UCDavis-Server-IPBlocking']
UCD_DNSP_ADC = ADC_db['UCDavis-Server-DNSPoisoning']

# 对于China-Mobile-DNSPoisoning, 数据格式为id, domain, result。 针对每一个domain的result进行统计分析, 生成一个summary插入到当前domain的document中

def process_CM_DNSP_ADC():
    logger.info("Processing China-Mobile-DNSPoisoning")
    # retrieve all documents
    documents = CM_DNSP_ADC.find()
    for document in documents:
        results = document['results']
        summary = {'result_ipv4': [], 'result_ipv6': []}
        previous_ipv4 = None
        previous_ipv6 = None
        for result in results:
            dns_server = result['dns_server']
            result_ipv4 = result['result_ipv4']
            result_ipv6 = result['result_ipv6']

            if result_ipv4 != previous_ipv4:
                summary['result_ipv4'].append({'dns_server': dns_server, 'result': result_ipv4})
                previous_ipv4 = result_ipv4

            if result_ipv6 != previous_ipv6:
                summary['result_ipv6'].append({'dns_server': dns_server, 'result': result_ipv6})
                previous_ipv6 = result_ipv6

        # Update the document with the summary
        CM_DNSP_ADC.update_one({'_id': document['_id']}, {'$set': {'summary': summary}})

process_CM_DNSP_ADC()
