import ast
import concurrent.futures
import gc
import logging
import multiprocessing
import re
from collections import defaultdict
from itertools import chain
from threading import Lock
from typing import Any, Dict, Generator, List, Optional, Set, Union

from .DBOperations import ADC_db, BDC_db, Merged_db, MongoDBHandler

try:
  from tqdm import tqdm
except ImportError:
  # 如果 tqdm 不可用，创建一个简单的替代
  def tqdm(iterable, desc=None, **kwargs):
    if desc:
      print(f"Processing: {desc}")
    return iterable


# Config Logger
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 优化的配置常量
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = min(CPU_CORES * 2, 32)  # 限制最大工作线程数，避免过度消耗资源
BATCH_SIZE = 5000  # 减少批处理大小，降低内存使用
MEMORY_LIMIT_MB = 1024  # 内存限制（MB）

# 内网IP模式（预编译正则表达式提高性能）
INTERNAL_IP_PATTERNS = [
    re.compile(r"^10\."),
    re.compile(r"^172\.(1[6-9]|2[0-9]|3[0-1])\."),
    re.compile(r"^192\.168\."),
    re.compile(r"^127\."),
    re.compile(r"^0\."),
    re.compile(r"^::1"),
    re.compile(r"^fe80"),
    re.compile(r"^fc00"),
    re.compile(r"^fd00")
]


# 数据库处理器初始化（延迟初始化减少内存使用）
def get_database_handlers():
  """延迟初始化数据库处理器"""
  return {
      # DNSPoisoning Constants for AfterDomainChange
      'ADC_CM_DNSP':
      MongoDBHandler(ADC_db["China-Mobile-DNSPoisoning"]),
      'ADC_CT_DNSP':
      MongoDBHandler(ADC_db["China-Telecom-DNSPoisoning"]),
      'ERROR_DOMAIN_DSP_ADC_CM':
      MongoDBHandler(ADC_db["ERROR_CODES"]),

      # TraceRoute Constants for AfterDomainChange
      'ADC_CM_GFWL':
      MongoDBHandler(ADC_db["China-Mobile-GFWLocation"]),
      'ADC_CT_GFWL':
      MongoDBHandler(ADC_db["China-Telecom-GFWLocation"]),
      'ADC_CT_IPB':
      MongoDBHandler(ADC_db["China-Telecom-IPBlocking"]),

      # DNSPoisoning Constants for BeforeDomainChange
      'BDC_CM_DNSP':
      MongoDBHandler(BDC_db["China-Mobile-DNSPoisoning"]),

      # TraceRoute Constants for BeforeDomainChange
      'BDC_CM_GFWL':
      MongoDBHandler(BDC_db["China-Mobile-GFWLocation"]),
      'BDC_CT_IPB':
      MongoDBHandler(BDC_db["China-Telecom-IPBlocking"]),

      # 2024 November Data Constants for AfterDocmainChange
      'ADC_CM_DNSP_NOV':
      MongoDBHandler(ADC_db["ChinaMobile-DNSPoisoning-November"]),
      'ADC_CM_GFWL_NOV':
      MongoDBHandler(ADC_db["ChinaMobile-GFWLocation-November"]),

      # 2025 Data Constants for AfterDocmainChange
      'ADC_CM_DNSP_2025':
      MongoDBHandler(ADC_db["ChinaMobile-DNSPoisoning-2025-January"]),
      'ADC_CM_GFWL_2025':
      MongoDBHandler(ADC_db["ChinaMobile-GFWLocation-2025-January"]),

      # Merged database handlers
      'Merged_db_DNSP':
      MongoDBHandler(Merged_db["DNSPoisoning"]),
      'Merged_db_TR':
      MongoDBHandler(Merged_db["TraceRouteResult"]),
      'Merged_db_2025_DNS':
      MongoDBHandler(Merged_db["2025_DNS"]),
      'Merged_db_2025_GFWL':
      MongoDBHandler(Merged_db["2025_GFWL"]),
      'Merged_db_2024_DNS':
      MongoDBHandler(Merged_db["2024_Nov_DNS"]),
      'Merged_db_2024_GFWL':
      MongoDBHandler(Merged_db["2024_Nov_GFWL"])
  }


class OptimizedMerger:
  """优化的数据合并器，提高性能和内存效率"""

  def __init__(self, db_handlers: Dict[str, MongoDBHandler]):
    self.db_handlers = db_handlers
    self.processed_domains_dnsp = defaultdict(lambda: defaultdict(set))
    self.processed_domains_dnsp_2024_NOV = defaultdict(
        lambda: defaultdict(set))
    self.processed_domains_dnsp_2025 = defaultdict(lambda: defaultdict(set))
    self.processed_domains_tr = defaultdict(lambda: defaultdict(set))
    self.processed_domains_tr_2024_NOV = defaultdict(lambda: defaultdict(set))
    self.processed_domains_tr_2025 = defaultdict(lambda: defaultdict(set))
    self.lock = Lock()
    self._error_code_cache = None

  def _get_error_code_data(self) -> Dict[str, Any]:
    """缓存错误代码数据，避免重复查询"""
    if self._error_code_cache is None:
      try:
        self._error_code_cache = {
            doc["domain"]: doc
            for doc in self.db_handlers['ERROR_DOMAIN_DSP_ADC_CM'].find({})
        }
      except Exception as e:
        logger.warning(f"Failed to load error code data: {e}")
        self._error_code_cache = {}
    return self._error_code_cache

  def _get_documents_generator(
      self, db_handler: MongoDBHandler) -> Generator[Dict, None, None]:
    """使用生成器减少内存使用"""
    try:
      # 使用cursor而不是将所有文档加载到内存
      cursor = db_handler.collection.find({})
      for document in cursor:
        yield document
    except Exception as e:
      logger.error(
          f"Error getting documents from {db_handler.collection.name}: {e}")

  def _is_ip_poisoned(self, ips: List[str]) -> bool:
    """优化的IP毒化检测"""
    if not ips:
      return False

    for ip in ips:
      if not isinstance(ip, str):
        continue
      for pattern in INTERNAL_IP_PATTERNS:
        if pattern.match(ip):
          return True
    return False

  def _clean_and_parse_ips(self, ips: Union[str, List[str]]) -> List[str]:
    """优化的IP清理和解析"""
    if not ips:
      return []

    all_ips = set()

    if isinstance(ips, str):
      try:
        cleaned_ips = ast.literal_eval(ips)
        if isinstance(cleaned_ips, list):
          all_ips.update(ip.strip() for ip in cleaned_ips
                         if isinstance(ip, str) and ip.strip())
        else:
          if isinstance(cleaned_ips, str) and cleaned_ips.strip():
            all_ips.add(cleaned_ips.strip())
      except (ValueError, SyntaxError):
        # 如果解析失败，尝试作为简单字符串处理
        if ips.strip():
          all_ips.add(ips.strip())
    elif isinstance(ips, list):
      for item in ips:
        if isinstance(item, str):
          try:
            cleaned_item = ast.literal_eval(item)
            if isinstance(cleaned_item, list):
              all_ips.update(ip.strip() for ip in cleaned_item
                             if isinstance(ip, str) and ip.strip())
            else:
              if isinstance(cleaned_item, str) and cleaned_item.strip():
                all_ips.add(cleaned_item.strip())
          except (ValueError, SyntaxError):
            if item.strip():
              all_ips.add(item.strip())

    return list(all_ips)


class OptimizedMerger:
  """优化的数据合并器，提高性能和内存效率"""

  def __init__(self, db_handlers: Dict[str, MongoDBHandler]):
    self.db_handlers = db_handlers
    self.processed_domains_dnsp = defaultdict(lambda: defaultdict(set))
    self.processed_domains_dnsp_2024_NOV = defaultdict(
        lambda: defaultdict(set))
    self.processed_domains_dnsp_2025 = defaultdict(lambda: defaultdict(set))
    self.processed_domains_tr = defaultdict(lambda: defaultdict(set))
    self.processed_domains_tr_2024_NOV = defaultdict(lambda: defaultdict(set))
    self.processed_domains_tr_2025 = defaultdict(lambda: defaultdict(set))
    self.lock = Lock()
    self._error_code_cache = None

  def _get_error_code_data(self) -> Dict[str, Any]:
    """缓存错误代码数据，避免重复查询"""
    if self._error_code_cache is None:
      try:
        self._error_code_cache = {
            doc["domain"]: doc
            for doc in self.db_handlers['ERROR_DOMAIN_DSP_ADC_CM'].find({})
        }
      except Exception as e:
        logger.warning(f"Failed to load error code data: {e}")
        self._error_code_cache = {}
    return self._error_code_cache

  def _get_documents_generator(
      self, db_handler: MongoDBHandler) -> Generator[Dict, None, None]:
    """使用生成器减少内存使用"""
    try:
      # 使用cursor而不是将所有文档加载到内存
      cursor = db_handler.collection.find({})
      for document in cursor:
        yield document
    except Exception as e:
      logger.error(
          f"Error getting documents from {db_handler.collection.name}: {e}")

  def _is_ip_poisoned(self, ips: List[str]) -> bool:
    """优化的IP毒化检测"""
    if not ips:
      return False

    for ip in ips:
      if not isinstance(ip, str):
        continue
      for pattern in INTERNAL_IP_PATTERNS:
        if pattern.match(ip):
          return True
    return False

  def _clean_and_parse_ips(self, ips: Union[str, List[str]]) -> List[str]:
    """优化的IP清理和解析"""
    if not ips:
      return []

    all_ips = set()

    if isinstance(ips, str):
      try:
        cleaned_ips = ast.literal_eval(ips)
        if isinstance(cleaned_ips, list):
          all_ips.update(ip.strip() for ip in cleaned_ips
                         if isinstance(ip, str) and ip.strip())
        else:
          if isinstance(cleaned_ips, str) and cleaned_ips.strip():
            all_ips.add(cleaned_ips.strip())
      except (ValueError, SyntaxError):
        # 如果解析失败，尝试作为简单字符串处理
        if ips.strip():
          all_ips.add(ips.strip())
    elif isinstance(ips, list):
      for item in ips:
        if isinstance(item, str):
          try:
            cleaned_item = ast.literal_eval(item)
            if isinstance(cleaned_item, list):
              all_ips.update(ip.strip() for ip in cleaned_item
                             if isinstance(ip, str) and ip.strip())
            else:
              if isinstance(cleaned_item, str) and cleaned_item.strip():
                all_ips.add(cleaned_item.strip())
          except (ValueError, SyntaxError):
            if item.strip():
              all_ips.add(item.strip())

    return list(all_ips)

  def merge_documents(self):
    """主要的文档合并方法"""
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_WORKERS) as executor:
      futures = []

      # DNSPoisoning 任务
      merge_tasks = [
          (self.db_handlers['ADC_CM_DNSP'], self._merge_adc_cm_dnsp,
           self.processed_domains_dnsp, self.db_handlers['Merged_db_DNSP'],
           True),
          (self.db_handlers['ADC_CT_DNSP'], self._merge_adc_ct_dnsp,
           self.processed_domains_dnsp, self.db_handlers['Merged_db_DNSP'],
           True),
          (self.db_handlers['BDC_CM_DNSP'], self._merge_bdc_cm_dnsp,
           self.processed_domains_dnsp, self.db_handlers['Merged_db_DNSP'],
           True),

          # TraceRoute 任务
          (self.db_handlers['ADC_CM_GFWL'], self._merge_adc_cm_gfwl,
           self.processed_domains_tr, self.db_handlers['Merged_db_TR'], False
           ),
          (self.db_handlers['ADC_CT_GFWL'], self._merge_adc_ct_gfwl,
           self.processed_domains_tr, self.db_handlers['Merged_db_TR'],
           False),
          (self.db_handlers['ADC_CT_IPB'], self._merge_adc_ct_ipb,
           self.processed_domains_tr, self.db_handlers['Merged_db_TR'],
           False),
          (self.db_handlers['BDC_CM_GFWL'], self._merge_bdc_cm_gfwl,
           self.processed_domains_tr, self.db_handlers['Merged_db_TR'],
           False),
          (self.db_handlers['BDC_CT_IPB'], self._merge_bdc_ct_ipb,
           self.processed_domains_tr, self.db_handlers['Merged_db_TR'],
           False),

          # 2025 数据任务
          (self.db_handlers['ADC_CM_DNSP_2025'], self._merge_adc_cm_dnsp,
           self.processed_domains_dnsp_2025,
           self.db_handlers['Merged_db_2025_DNS'], True),
          (self.db_handlers['ADC_CM_GFWL_2025'], self._merge_adc_cm_gfwl,
           self.processed_domains_tr_2025,
           self.db_handlers['Merged_db_2025_GFWL'], False),

          # 2024 November 数据任务
          (self.db_handlers['ADC_CM_DNSP_NOV'], self._merge_adc_cm_dnsp_nov,
           self.processed_domains_dnsp_2024_NOV,
           self.db_handlers['Merged_db_2024_DNS'], True),
          (self.db_handlers['ADC_CM_GFWL_NOV'], self._merge_adc_cm_gfwl_nov,
           self.processed_domains_tr_2024_NOV,
           self.db_handlers['Merged_db_2024_GFWL'], False),
      ]

      for db_handler, merge_function, processed_domains, target_db, use_dns_server in merge_tasks:
        future = executor.submit(self._merge_documents, db_handler,
                                 merge_function, processed_domains, target_db,
                                 use_dns_server)
        futures.append(future)

      # 等待所有任务完成
      for future in concurrent.futures.as_completed(futures):
        try:
          future.result()
        except Exception as e:
          logger.error(f"Error in thread execution: {e}")

    # 最终化文档
    self._finalize_documents(self.processed_domains_dnsp,
                             self.db_handlers['Merged_db_DNSP'],
                             is_traceroute=False,
                             use_dns_server=True)
    self._finalize_documents(self.processed_domains_tr,
                             self.db_handlers['Merged_db_TR'],
                             is_traceroute=True)
    self._finalize_documents(self.processed_domains_dnsp_2025,
                             self.db_handlers['Merged_db_2025_DNS'],
                             is_traceroute=False,
                             use_dns_server=True)
    self._finalize_documents(self.processed_domains_tr_2025,
                             self.db_handlers['Merged_db_2025_GFWL'],
                             is_traceroute=True)
    self._finalize_documents(self.processed_domains_dnsp_2024_NOV,
                             self.db_handlers['Merged_db_2024_DNS'],
                             is_traceroute=False,
                             use_dns_server=True)
    self._finalize_documents(self.processed_domains_tr_2024_NOV,
                             self.db_handlers['Merged_db_2024_GFWL'],
                             is_traceroute=True)

  def _merge_documents(self,
                       db_handler,
                       merge_function,
                       processed_domains,
                       target_db,
                       use_dns_server=False):
    """合并文档的通用方法"""
    logger.info(f"Merging documents from {db_handler.collection.name}")
    try:
      document_count = 0
      for document in self._get_documents_generator(db_handler):
        merge_function(document, processed_domains, use_dns_server)
        document_count += 1

        # 定期强制垃圾回收
        if document_count % 1000 == 0:
          gc.collect()

    except Exception as e:
      logger.error(f"Error in _merge_documents: {e}")

  def _merge_adc_cm_dnsp(self, document, processed_domains, use_dns_server):
    """合并 ADC China Mobile DNS Poisoning 数据"""
    self._process_document(
        self._format_dns_document(domain=document.get("domain", ""),
                                  timestamp=document.get("timestamp", []),
                                  ips=document.get("ips", []),
                                  dns_server=document.get(
                                      "dns_server", "unknown")),
        processed_domains, use_dns_server)

  def _merge_adc_cm_gfwl(self, document, processed_domains, use_dns_server):
    """合并 ADC China Mobile GFW Location 数据"""
    self._process_document(
        self._format_traceroute_document(domain=document.get("domain", ""),
                                         ips=document.get("ips", []),
                                         error=document.get("error", []),
                                         mark=document.get("mark", [])),
        processed_domains, use_dns_server)

  def _merge_adc_ct_dnsp(self, document, processed_domains, use_dns_server):
    """合并 ADC China Telecom DNS Poisoning 数据"""
    self._process_document(
        self._format_dns_document(domain=document.get("domain", ""),
                                  timestamp=document.get("timestamp", []),
                                  ips=document.get("ips", []),
                                  dns_server=document.get(
                                      "dns_server", "unknown")),
        processed_domains, use_dns_server)

  def _merge_adc_ct_gfwl(self, document, processed_domains, use_dns_server):
    """合并 ADC China Telecom GFW Location 数据"""
    self._process_document(
        self._format_traceroute_document(domain=document.get("domain", ""),
                                         ips=document.get("results", [])),
        processed_domains, use_dns_server)

  def _merge_adc_ct_ipb(self, document, processed_domains, use_dns_server):
    """合并 ADC China Telecom IP Blocking 数据"""
    self._process_document(
        self._format_traceroute_document(
            domain=document.get("domain", ""),
            timestamp=document.get("timestamp", []),
            ipv4=document.get("IPv4", []),
            ipv6=document.get("IPv6", []),
            is_accessible=document.get("is_accessible", [])),
        processed_domains, use_dns_server)

  def _merge_adc_cm_dnsp_nov(self, document, processed_domains,
                             use_dns_server):
    """合并 ADC China Mobile DNS Poisoning November 数据"""
    self._process_document(
        self._format_dns_document(
            domain=document.get("domain", ""),
            timestamp=document.get("timestamp", []),
            dns_server=document.get("dns_server", "unknown"),
            ips=document.get("ips", []),
            error_code=document.get("error_code", []),
            error_reason=document.get("error_reason", []),
            record_type=document.get("record_type", [])), processed_domains,
        use_dns_server)

  def _merge_adc_cm_gfwl_nov(self, document, processed_domains,
                             use_dns_server):
    """合并 ADC China Mobile GFW Location November 数据"""
    self._process_document(
        self._format_traceroute_document(
            domain=document.get("domain", ""),
            error=document.get("Error", []),
            ipv4=document.get("IPv4", []),
            ipv6=document.get("IPv6", []),
            invalid_ip=document.get("Invalid IP", []),
            rst_detected=document.get("RST Detected", []),
            redirection_detected=document.get("Redirection Detected", []),
            timestamp=document.get("timestamp", [])), processed_domains,
        use_dns_server)

  def _merge_bdc_cm_dnsp(self, document, processed_domains, use_dns_server):
    """合并 BDC China Mobile DNS Poisoning 数据"""
    self._process_document(
        self._format_dns_document(domain=document.get("domain", ""),
                                  timestamp=document.get("timestamp", []),
                                  ips=document.get("ips", []),
                                  dns_server=document.get(
                                      "dns_server", "unknown")),
        processed_domains, use_dns_server)

  def _merge_bdc_cm_gfwl(self, document, processed_domains, use_dns_server):
    """合并 BDC China Mobile GFW Location 数据"""
    self._process_document(
        self._format_traceroute_document(domain=document.get("domain", ""),
                                         ips=document.get("result", []),
                                         dns_server=document.get(
                                             "dns_server", "unknown")),
        processed_domains, use_dns_server)

  def _merge_bdc_ct_ipb(self, document, processed_domains, use_dns_server):
    """合并 BDC China Telecom IP Blocking 数据"""
    self._process_document(
        self._format_traceroute_document(
            domain=document.get("domain", ""),
            timestamp=document.get("timestamp", []),
            results_ip=document.get("results_ip", []),
            ip_type=document.get("ip_type", []),
            port=document.get("port", []),
            is_accessible=document.get("is_accessible", []),
            dns_server=document.get("dns_server", "unknown")),
        processed_domains, use_dns_server)

  def _format_dns_document(self,
                           domain: str,
                           timestamp=None,
                           ips=None,
                           dns_server=None,
                           error_code=None,
                           error_reason=None,
                           record_type=None) -> Dict[str, Any]:
    """格式化DNS文档"""
    unique_ips = self._clean_and_parse_ips(ips)
    is_poisoned = self._is_ip_poisoned(unique_ips)

    return {
        "domain": domain,
        "timestamp": timestamp or [],
        "ips": unique_ips,
        "dns_server": dns_server or "unknown",
        "error_code": error_code or [],
        "error_reason": error_reason or [],
        "record_type": record_type or [],
        "is_poisoned": is_poisoned,
    }

  def _format_traceroute_document(self,
                                  domain: str,
                                  timestamp=None,
                                  ips=None,
                                  error=None,
                                  mark=None,
                                  ipv4=None,
                                  ipv6=None,
                                  invalid_ip=None,
                                  rst_detected=None,
                                  redirection_detected=None,
                                  results_ip=None,
                                  ip_type=None,
                                  port=None,
                                  is_accessible=None,
                                  dns_server=None) -> Dict[str, Any]:
    """格式化TraceroUte文档"""
    return {
        "domain": domain,
        "timestamp": timestamp or [],
        "ips": ips or [],
        "error": error or [],
        "mark": mark or [],
        "IPv4": ipv4 or [],
        "IPv6": ipv6 or [],
        "invalid_ip": invalid_ip or [],
        "rst_detected": rst_detected or [],
        "redirection_detected": redirection_detected or [],
        "results_ip": results_ip or [],
        "ip_type": ip_type or [],
        "port": port or [],
        "is_accessible": is_accessible or [],
        "dns_server": dns_server or "unknown"
    }

  def _process_document(self,
                        document: Dict[str, Any],
                        processed_domains,
                        use_dns_server=False):
    """处理单个文档"""
    try:
      domain = document["domain"]
      if not domain:
        logger.warning(f"Document with missing domain skipped: {document}")
        return

      dns_server = document.get("dns_server", "unknown")
      if isinstance(dns_server, str):
        try:
          dns_servers = ast.literal_eval(dns_server)
        except (ValueError, SyntaxError):
          dns_servers = [dns_server]
      else:
        dns_servers = [dns_server] if dns_server else ["unknown"]

      with self.lock:
        for dns_srv in dns_servers:
          key = (domain, dns_srv) if use_dns_server else domain
          for field, value in document.items():
            if field not in ["domain", "dns_server"]:
              if isinstance(value, list):
                flat_values = set(
                    chain.from_iterable(v if isinstance(v, list) else [v]
                                        for v in value if v))
                processed_domains[key][field].update(flat_values)
              else:
                if value:
                  processed_domains[key][field].add(value)
    except Exception as e:
      logger.error(f"Error processing document: {document}, {e}")

  def _finalize_documents(self,
                          processed_domains,
                          target_db,
                          is_traceroute=False,
                          use_dns_server=False):
    """最终化文档并批量插入"""
    batch = []
    counter = 0
    error_code_data = self._get_error_code_data()

    for key, data in processed_domains.items():
      try:
        if use_dns_server:
          domain, dns_server = key
          if not dns_server or len(dns_server) <= 1:
            continue
        else:
          domain = key
          dns_server = None

        logger.info(
            f"Processing domain: {domain}, target_db: {target_db.collection.name}"
        )

        finalized_document = self._create_finalized_document(
            domain, dns_server, data, target_db, is_traceroute, counter,
            error_code_data)

        if finalized_document:
          batch.append(finalized_document)
          counter += 1

          if len(batch) >= BATCH_SIZE:
            self._insert_documents(batch, target_db)
            batch = []
            gc.collect()  # 强制垃圾回收

      except Exception as e:
        logger.error(f"Error finalizing document for domain {key}: {e}")

    if batch:
      self._insert_documents(batch, target_db)

  def _create_finalized_document(self, domain, dns_server, data, target_db,
                                 is_traceroute, counter, error_code_data):
    """创建最终化的文档"""
    base_id = f"{target_db.collection.name}-{domain}-{counter}"

    if target_db.collection.name in [
        "2025_GFWL", "2024_Nov_GFWL", "2025_DNS", "2024_Nov_DNS"
    ]:
      # 新格式文档
      return {
          "_id": base_id,
          "domain": domain,
          "dns_server": dns_server,
          "ips": list(data.get("ips", [])),
          "error_code": list(data.get("error_code", [])),
          "error_reason": list(data.get("error_reason", [])),
          "record_type": list(data.get("record_type", [])),
          "timestamp": list(data.get("timestamp", [])),
          **{
              field: list(value)
              for field, value in data.items() if field not in [
                  "domain", "dns_server", "ips", "error_code", "error_reason", "record_type", "timestamp"
              ]
          }
      }
    else:
      # 传统格式文档
      if is_traceroute:
        finalized_document = {
            "_id":
            f"TRACEROUTE-{base_id}",
            "domain":
            domain,
            "timestamp":
            list(data.get("timestamp", [])),
            "ips":
            list(data.get("ips", [])) + list(data.get("IPv4", [])) +
            list(data.get("IPv6", [])),
            "error":
            list(data.get("error", [])),
            "error_reason":
            list(data.get("Error Reason", [])),
            "mark":
            list(data.get("mark", [])),
            "results":
            list(data.get("results", [])),
            "is_accessible":
            list(data.get("is_accessible", [])),
        }
        self._process_traceroute_errors(finalized_document, data)
      else:
        finalized_document = {
            "_id": f"DNSPOISON-{base_id}",
            "domain": domain,
            "dns_server": dns_server,
            "ips": list(data.get("ips", [])),
            "error_code": list(data.get("error_code", [])),
            "error_reason": list(data.get("error_reason", [])),
            "record_type": list(data.get("record_type", [])),
            "timestamp": list(data.get("timestamp", [])),
            "is_poisoned": bool(data.get("is_poisoned", False)),
        }

        # 添加错误代码信息
        if domain in error_code_data:
          error_info = error_code_data[domain]
          finalized_document["dns_server"] = error_info.get("dns_server", [])
          finalized_document["error_code"] = error_info.get("error_code", [])
          finalized_document["error_reason"] = error_info.get(
              "error_reason", [])
          # 过滤无效错误代码
          finalized_document["error_code"] = [
              ec for ec in finalized_document["error_code"]
              if ec.lower() not in ["erying", "former", "refuse"]
          ]

      # 添加其他字段
      for field, value in data.items():
        if field not in finalized_document:
          finalized_document[field] = list(value)

      return finalized_document

  def _process_traceroute_errors(self, finalized_document, data):
    """处理traceroute特定的错误"""
    # 检查是否包含内网地址
    if '127.0.0.1' in data.get('IPv4', []) or '::1' in data.get('IPv6', []):
      finalized_document['error'].append('Blocked')
      finalized_document['error_reason'].append('Internal IP Address Blocked')

    # 错误映射
    error_mappings = {
        'Traceroute timed out': 'Timeout',
        'No Answer': 'NoAnswer',
        'Traceroute Failed': 'Failed',
        'Not Found': 'NotFound',
        'Network Unreachable': 'NetworkUnreachable',
        'Host Unreachable': 'HostUnreachable',
        'Protocol Unreachable': 'ProtocolUnreachable',
        'Port Unreachable': 'PortUnreachable',
        'Fragmentation Needed': 'FragmentationNeeded',
        'Source Route Failed': 'SourceRouteFailed',
        'Destination Network Unknown': 'DestinationNetworkUnknown',
        'Destination Host Unknown': 'DestinationHostUnknown',
        'Source Host Isolated': 'SourceHostIsolated',
        'Communication with Destination Network Administratively Prohibited':
        'CommunicationWithDestinationNetworkAdministrativelyProhibited',
        'Communication with Destination Host Administratively Prohibited':
        'CommunicationWithDestinationHostAdministrativelyProhibited',
        'Destination Network Unreachable for Type of Service':
        'DestinationNetworkUnreachableForTypeOfService',
        'Destination Host Unreachable for Type of Service':
        'DestinationHostUnreachableForTypeOfService',
        'Communication Administratively Prohibited':
        'CommunicationAdministrativelyProhibited',
        'Host Precedence Violation': 'HostPrecedenceViolation',
        'Precedence cutoff in effect': 'PrecedenceCutoffInEffect'
    }

    for error in data.get('results', []):
      if error in error_mappings:
        finalized_document['error'].append(error_mappings[error])

  def _insert_documents(self, batch: List[Dict], target_db: MongoDBHandler):
    """批量插入文档"""
    try:
      if batch:
        logger.info(
            f"Inserting batch of {len(batch)} documents into {target_db.collection.name}"
        )
        target_db.insert_many(batch)
    except Exception as e:
      logger.error(f"Error inserting documents: {e}")


def main():
  """主函数"""
  try:
    logger.info("Starting OptimizedMerger")

    # 获取数据库处理器
    db_handlers = get_database_handlers()

    # 清空合并后的集合
    logger.info("Clearing merged collections...")
    collections_to_clear = [
        'Merged_db_DNSP', 'Merged_db_TR', 'Merged_db_2025_DNS',
        'Merged_db_2025_GFWL', 'Merged_db_2024_DNS', 'Merged_db_2024_GFWL'
    ]

    for collection_name in collections_to_clear:
      try:
        db_handlers[collection_name].collection.drop()
        logger.info(f"Cleared {collection_name}")
      except Exception as e:
        logger.warning(f"Failed to clear {collection_name}: {e}")

    logger.info("Merged collections cleared")

    # 创建优化的合并器实例
    merger = OptimizedMerger(db_handlers)

    # 执行合并
    merger.merge_documents()

    logger.info("OptimizedMerger completed successfully")

  except Exception as e:
    logger.error(f"Error in OptimizedMerger: {e}")
    raise


if __name__ == "__main__":
  main()
