import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

from pymongo.collection import Collection

from ..Utils.db_operations import BDC_db, MongoDBHandler

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

class ArrayNullProcessor:
    def __init__(self):
        # 初始化数据库集合
        self.collections = {
            'CM_DNSP': BDC_db["China-Mobile-DNSPoisoning"],
            'CM_GFWL': BDC_db["China-Mobile-GFWLocation"],
            'CM_IPB': BDC_db["China-Mobile-IPBlocking"],
            'CT_IPB': BDC_db["China-Telecom-IPBlocking"],
            'UCD_CG_DNSP': BDC_db["UCDavis-CompareGroup-DNSPoisoning"],
            'UCD_CG_GFWL': BDC_db["UCDavis-CompareGroup-GFWLocation"],
            'UCD_CG_IPB': BDC_db["UCDavis-CompareGroup-IPBlocking"]
        }

    def find_and_clean_arrays(self, data: Any, path: str = "") -> Tuple[Dict[str, int], Any]:
        """查找并清理所有数组中的null值"""
        null_counts = {}

        if isinstance(data, dict):
            processed_dict = {}
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                sub_counts, processed_value = self.find_and_clean_arrays(value, current_path)
                null_counts.update(sub_counts)
                processed_dict[key] = processed_value
            return null_counts, processed_dict

        elif isinstance(data, list):
            # 计算当前数组中的null值
            null_count = sum(1 for item in data if item is None)
            if null_count > 0:
                null_counts[path] = null_count

            # 清理当前数组中的null值
            cleaned_array = [item for item in data if item is not None]

            # 如果数组元素是复杂类型，继续处理
            processed_array = []
            for item in cleaned_array:
                if isinstance(item, (dict, list)):
                    sub_counts, processed_item = self.find_and_clean_arrays(item, path)
                    null_counts.update(sub_counts)
                    processed_array.append(processed_item)
                else:
                    processed_array.append(item)

            return null_counts, processed_array

        return null_counts, data

    def process_collection(self, collection_name: str):
        """处理单个集合"""
        logger.info(f"\n开始处理集合: {collection_name}")
        collection = self.collections[collection_name]

        stats = {
            'total_docs': 0,
            'docs_with_nulls': 0,
            'total_nulls': 0,
            'arrays_with_nulls': {}
        }

        try:
            cursor = collection.find({})
            for doc in cursor:
                stats['total_docs'] += 1

                # 处理文档中的数组
                null_counts, processed_doc = self.find_and_clean_arrays(doc)

                if null_counts:
                    stats['docs_with_nulls'] += 1
                    doc_id = doc.get('domain', doc.get('ip', str(doc['_id'])))

                    # 更新统计信息
                    for path, count in null_counts.items():
                        stats['total_nulls'] += count
                        if path not in stats['arrays_with_nulls']:
                            stats['arrays_with_nulls'][path] = {
                                'total_nulls': 0,
                                'affected_docs': 0
                            }
                        stats['arrays_with_nulls'][path]['total_nulls'] += count
                        stats['arrays_with_nulls'][path]['affected_docs'] += 1

                    # 输出每个文档的统计信息
                    logger.info(f"\n文档 {doc_id}:")
                    for path, count in null_counts.items():
                        logger.info(f"  - {path}: 发现 {count} 个null值")

                    # 更新文档
                    collection.update_one(
                        {'_id': doc['_id']},
                        {'$set': processed_doc}
                    )

            # 输出集合统计信息
            logger.info(f"\n集合 {collection_name} 统计摘要:")
            logger.info(f"总文档数: {stats['total_docs']}")
            logger.info(f"包含null值的文档数: {stats['docs_with_nulls']}")
            logger.info(f"null值总数: {stats['total_nulls']}")

            if stats['arrays_with_nulls']:
                logger.info("\n各数组路径的null值统计:")
                for path, info in stats['arrays_with_nulls'].items():
                    logger.info(f"\n路径: {path}")
                    logger.info(f"  - 总计 {info['total_nulls']} 个null值")
                    logger.info(f"  - 影响了 {info['affected_docs']} 个文档")

        except Exception as e:
            logger.error(f"处理集合 {collection_name} 时发生错误: {str(e)}")
            raise

    def process_all_collections(self):
        """处理所有集合"""
        start_time = datetime.now()
        logger.info("开始处理所有集合")

        for collection_name in self.collections:
            try:
                self.process_collection(collection_name)
            except Exception as e:
                logger.error(f"处理集合 {collection_name} 失败: {str(e)}")
                continue

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"\n所有集合处理完成，总耗时: {duration}")

def main():
    processor = ArrayNullProcessor()
    processor.process_all_collections()

if __name__ == "__main__":
    main()
