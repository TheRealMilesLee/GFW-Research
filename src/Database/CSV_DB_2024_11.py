import concurrent.futures
import csv
import logging
import os
from collections import defaultdict

from DBOperations import ADC_db, MongoDBHandler

CM_DNSP_ADC_NOV = ADC_db['ChinaMobile-DNSPoisoning-November']

# Config Logger
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 设置基本配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 创建 logger
logger = logging.getLogger(__name__)

def process_file(file, mongodbOP_CM_DNSP):
    with open(file, 'r') as csvfile:
        logger.info(f'Processing file: {os.path.basename(file)}')
        reader = csv.DictReader(csvfile)
        data_dict = defaultdict(lambda: defaultdict(list))

        # 将数据分类到字典中，准备合并
        for row in reader:
            key = row['domain']
            data_dict[key]['timestamp'].append(row['timestamp'])
            data_dict[key]['record_type'].append(row['record_type'])
            data_dict[key]['answers'].append(row['answers'])
            data_dict[key]['error_code'].append(row['error_code'])
            data_dict[key]['error_reason'].append(row['error_reason'])
            data_dict[key]['dns_server'].append(row['dns_server'])

        # 逐个域名处理数据并更新到MongoDB
        for domain, value in data_dict.items():
            data = {
                'timestamp': list(set(value['timestamp'])),
                'record_type': list(set(value['record_type'])),
                'answers': list(set([ans for ans in value['answers'] if ans])),
                'error_code': list(set([code for code in value['error_code'] if code])),
                'error_reason': list(set([reason for reason in value['error_reason'] if reason])),
                'dns_server': list(set(value['dns_server']))
            }

            # 使用 $addToSet 确保数组中的唯一值
            update_data = {
                '$addToSet': {
                    'timestamp': {'$each': data['timestamp']},
                    'record_type': {'$each': data['record_type']},
                    'answers': {'$each': data['answers']},
                    'error_code': {'$each': data['error_code']},
                    'error_reason': {'$each': data['error_reason']},
                    'dns_server': {'$each': data['dns_server']}
                }
            }

            logger.info(f'Inserting DNSPoisoning data into MongoDB with domain: {domain}')
            mongodbOP_CM_DNSP.update_one({'domain': domain}, update_data, upsert=True)

def dump_to_mongo():
    mongodbOP_CM_DNSP = MongoDBHandler(CM_DNSP_ADC_NOV)

    if os.name == 'nt':
      FileFolderLocation = 'E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\Data-2024-11\\ChinaMobile'
    else:
      FileFolderLocation = '/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/Data-2024-11/ChinaMobile'
    csv_files = [os.path.join(FileFolderLocation, file) for file in os.listdir(FileFolderLocation) if file.endswith('.csv')]

    # Drop the collection before inserting new data
    logger.info('Dropping the collection before inserting new data')
    CM_DNSP_ADC_NOV.drop()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_file, file, mongodbOP_CM_DNSP) for file in csv_files]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f'Error processing file: {e}')

if __name__ == '__main__':
    dump_to_mongo()
