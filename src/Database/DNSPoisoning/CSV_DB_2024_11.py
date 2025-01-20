import concurrent.futures
import csv
import logging
import os
import multiprocessing
from collections import defaultdict
import ast

from ..DBOperations import ADC_db, MongoDBHandler
from tqdm import tqdm
CPU_CORES = multiprocessing.cpu_count()
CM_DNSP_ADC_NOV = ADC_db['ChinaMobile-DNSPoisoning-November']
MAX_WORKERS = max(CPU_CORES * 2, 64)  # Dynamically set workers
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
            try:
                dns_servers = ast.literal_eval(row['dns_server'])  # 使用 ast.literal_eval 安全地将字符串转换为列表
            except (ValueError, SyntaxError):
                dns_servers = [row['dns_server']]  # 如果转换失败，则将其视为单个 DNS 服务器
            for dns_server in dns_servers:
                key = (row['domain'], dns_server)  # 使用(domain, dns_server)作为唯一键
                data_dict[key]['timestamp'].append(row['timestamp'])
                data_dict[key]['record_type'].append(row['record_type'])
                data_dict[key]['answers'].append(row['answers'])
                data_dict[key]['error_code'].append(row['error_code'])
                data_dict[key]['error_reason'].append(row['error_reason'])

        # 逐个域名处理数据并更新到MongoDB
        for (domain, dns_server), value in tqdm(data_dict.items(), desc=f'Inserting data from {os.path.basename(file)}'):
            data = {
                'timestamp': value['timestamp'],  # 不聚合 timestamp
                'record_type': list(set(value['record_type'])),
                'answers': sorted(list(set([ans for ans in value['answers'] if ans]))),
                'error_code': list(set([code for code in value['error_code'] if code])),
                'error_reason': list(set([reason for reason in value['error_reason'] if reason])),
                'dns_server': dns_server
            }

            # 使用 $addToSet 确保数组中的唯一值
            update_data = {
                '$addToSet': {
                    'timestamp': {'$each': data['timestamp']},
                    'record_type': {'$each': data['record_type']},
                    'answers': {'$each': data['answers']},
                    'error_code': {'$each': data['error_code']},
                    'error_reason': {'$each': data['error_reason']}
                }
            }

            mongodbOP_CM_DNSP.update_one({'domain': domain, 'dns_server': dns_server}, update_data, upsert=True)

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

    # Create an index for the domain, dns_server, and timestamp fields
    logger.info('Creating index for the domain, dns_server, and timestamp fields')
    CM_DNSP_ADC_NOV.create_index([('domain', 1), ('dns_server', 1), ('timestamp', 1)], unique=False)  # 创建包含timestamp的复合唯一索引

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, file, mongodbOP_CM_DNSP) for file in csv_files]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc='Processing files'):
            try:
                future.result()
            except Exception as e:
                logger.error(f'Error processing file: {e}')

if __name__ == '__main__':
    dump_to_mongo()
