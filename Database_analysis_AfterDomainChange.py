from src.Dump.db_operations import ADC_db, MongodbHandler
import logging

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
    for domain in CM_DNSP_ADC.find():
        result = domain['result']
        summary = {
            "total": len(result),
            "poisoned": len([r for r in result if r == "poisoned"]),
            "clean": len([r for r in result if r == "clean"]),
            "unknown": len([r for r in result if r == "unknown"])
        }
        CM_DNSP_ADC.update_one({"_id": domain["_id"]}, {"$set": {"summary": summary}})
    logger.info("China-Mobile-DNSPoisoning Done")
