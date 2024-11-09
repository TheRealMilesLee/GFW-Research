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

