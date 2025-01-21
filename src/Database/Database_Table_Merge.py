import concurrent.futures
import datetime
import logging
import multiprocessing
import re
import ast
from collections import defaultdict
from itertools import chain
from threading import Lock
import pymongo
from .DBOperations import ADC_db, BDC_db, CompareGroup_db, Merged_db, MongoDBHandler
from tqdm import tqdm
# Config Logger
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', filename='out.log', filemode='w')  # 修正格式字符串并输出到out.log
logger = logging.getLogger(__name__)

# DNSPoisoning Constants for AfterDomainChange
ADC_CM_DNSP = MongoDBHandler(ADC_db["China-Mobile-DNSPoisoning"])
ADC_CT_DNSP = MongoDBHandler(ADC_db["China-Telecom-DNSPoisoning"])
ERROR_DOMAIN_DSP_ADC_CM = MongoDBHandler(ADC_db["ERROR_CODES"])

# TraceRoute Constants for AfterDomainChange
ADC_CM_GFWL = MongoDBHandler(ADC_db["China-Mobile-GFWLocation"])
ADC_CT_GFWL = MongoDBHandler(ADC_db["China-Telecom-GFWLocation"])
ADC_CT_IPB = MongoDBHandler(ADC_db["China-Telecom-IPBlocking"])

# DNSPoisoning Constants for BeforeDomainChange
BDC_CM_DNSP = MongoDBHandler(BDC_db["China-Mobile-DNSPoisoning"])

# TraceRoute Constants for BeforeDomainChange
BDC_CM_GFWL = MongoDBHandler(BDC_db["China-Mobile-GFWLocation"])
BDC_CT_IPB = MongoDBHandler(BDC_db["China-Telecom-IPBlocking"])

# 2024 November Data Constants for AfterDocmainChange (Placed in another table named 2024)
ADC_CM_DNSP_NOV = MongoDBHandler(ADC_db["ChinaMobile-DNSPoisoning-November"])
ADC_CM_GFWL_NOV = MongoDBHandler(ADC_db["ChinaMobile-GFWLocation-November"])

# 2025 Data Constants for AfterDocmainChange (Placed in another table named 2025)
ADC_CM_DNSP_2025 = MongoDBHandler(ADC_db["ChinaMobile-DNSPoisoning-2025-January"]) # DNSPosioning
ADC_CM_GFWL_2025 = MongoDBHandler(ADC_db["ChinaMobile-GFWLocation-2025-January"]) # TraceRoute

# CompareGroup DNSPoisoning Constants
UCDS_DNSP = MongoDBHandler(ADC_db["UCDavis-Server-DNSPoisoning"])
BDC_UCDS_DNSP = MongoDBHandler(BDC_db["UCDavis-CompareGroup-DNSPoisoning"])

# CompareGroup TraceRoute Constants
UCDS_GFWL = MongoDBHandler(ADC_db["UCDavis-Server-GFWLocation"])
UCDS_IPB = MongoDBHandler(ADC_db["UCDavis-Server-IPBlocking"])
BDC_UCDS_GFWL = MongoDBHandler(BDC_db["UCDavis-CompareGroup-GFWLocation"])
BDC_UCDS_IPB = MongoDBHandler(BDC_db["UCDavis-CompareGroup-IPBlocking"])

# Merged database handler
Merged_db_DNSP = MongoDBHandler(Merged_db["DNSPoisoning"])
Merged_db_TR = MongoDBHandler(Merged_db["TraceRouteResult"])
Merged_db_2025_DNS = MongoDBHandler(Merged_db["2025_DNS"])  # 新增
Merged_db_2025_GFWL = MongoDBHandler(Merged_db["2025_GFWL"])  # 新增
Merged_db_2024_DNS = MongoDBHandler(Merged_db["2024_Nov_DNS"])  # 新增
Merged_db_2024_GFWL = MongoDBHandler(Merged_db["2024_Nov_GFWL"])  # 新增

# CompareGroup database handler
CompareGroup_db_DNSP = MongoDBHandler(CompareGroup_db["DNSPoisoning"])
CompareGroup_db_TR = MongoDBHandler(CompareGroup_db["TraceRouteResult"])
# Optimize worker count based on CPU cores
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 512)  # Dynamically set workers
BATCH_SIZE = 100000  # Increased batch size for more efficient processing

class Merger:
  def __init__(
    self,
    adc_cm_dnsp,
    adc_cm_gfwl,
    adc_ct_dnsp,
    adc_ct_gfwl,
    adc_ct_ipb,
    adc_cm_dnsp_nov,
    adc_cm_gfwl_nov,
    error_domain_dsp_adc_cm,
    ucds_dnsp,
    ucds_gfwl,
    ucds_ipb,
    bdc_cm_dnsp,
    bdc_cm_gfwl,
    bdc_ct_ipb,
    bdc_ucds_dnsp,
    bdc_ucds_gfwl,
    bdc_ucds_ipb,
    merged_db_dnsp,
    merged_db_tr,
    comparegroup_db_dnsp,
    comparegroup_db_tr,
    adc_cm_dnsp_2025,
    adc_cm_gfwl_2025,
    merged_db_2025_dns,       # 新增
    merged_db_2025_gfwl,      # 新增
    merged_db_2024_dns,       # 新增
    merged_db_2024_gfwl,      # 新增
  ):
    self.adc_cm_dnsp = adc_cm_dnsp
    self.adc_cm_gfwl = adc_cm_gfwl
    self.adc_ct_dnsp = adc_ct_dnsp
    self.adc_ct_gfwl = adc_ct_gfwl
    self.adc_ct_ipb = adc_ct_ipb
    self.adc_cm_dnsp_nov = adc_cm_dnsp_nov
    self.adc_cm_gfwl_nov = adc_cm_gfwl_nov
    self.error_domain_dsp_adc_cm = error_domain_dsp_adc_cm
    self.ucds_dnsp = ucds_dnsp
    self.ucds_gfwl = ucds_gfwl
    self.ucds_ipb = ucds_ipb
    self.bdc_cm_dnsp = bdc_cm_dnsp
    self.bdc_cm_gfwl = bdc_cm_gfwl
    self.bdc_ct_ipb = bdc_ct_ipb
    self.bdc_ucds_dnsp = bdc_ucds_dnsp
    self.bdc_ucds_gfwl = bdc_ucds_gfwl
    self.bdc_ucds_ipb = bdc_ucds_ipb
    self.merged_db_dnsp = merged_db_dnsp
    self.merged_db_tr = merged_db_tr
    self.comparegroup_db_dnsp = comparegroup_db_dnsp
    self.comparegroup_db_tr = comparegroup_db_tr
    self.adc_cm_dnsp_2025 = adc_cm_dnsp_2025
    self.adc_cm_gfwl_2025 = adc_cm_gfwl_2025
    self.merged_db_2025_dns = merged_db_2025_dns       # 新增
    self.merged_db_2025_gfwl = merged_db_2025_gfwl     # 新增
    self.merged_db_2024_dns = merged_db_2024_dns       # 新增
    self.merged_db_2024_gfwl = merged_db_2024_gfwl     # 新增
    self.processed_domains_dnsp = defaultdict(lambda: defaultdict(set))
    self.processed_domains_tr = defaultdict(lambda: defaultdict(set))
    self.lock = Lock()

  def merge_documents(self):
    logger.info("Starting merge_documents")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      futures = [
        # DNSPoisoning Constants
        executor.submit(self._merge_documents, self.adc_cm_dnsp, self._merge_adc_cm_dnsp, self.processed_domains_dnsp, self.merged_db_dnsp, use_dns_server=True),
        executor.submit(self._merge_documents, self.adc_ct_dnsp, self._merge_adc_ct_dnsp, self.processed_domains_dnsp, self.merged_db_dnsp, use_dns_server=True),
        executor.submit(self._merge_documents, self.ucds_dnsp, self._merge_ucds_dnsp, self.processed_domains_dnsp, self.comparegroup_db_dnsp, use_dns_server=True),
        executor.submit(self._merge_documents, self.bdc_cm_dnsp, self._merge_bdc_cm_dnsp, self.processed_domains_dnsp, self.merged_db_dnsp, use_dns_server=True),
        executor.submit(self._merge_documents, self.bdc_ucds_dnsp, self._merge_bdc_ucds_dnsp, self.processed_domains_dnsp, self.comparegroup_db_dnsp, use_dns_server=True),
        # TraceRoute Constants
        executor.submit(self._merge_documents, self.adc_cm_gfwl, self._merge_adc_cm_gfwl, self.processed_domains_tr, self.merged_db_tr),
        executor.submit(self._merge_documents, self.adc_ct_gfwl, self._merge_adc_ct_gfwl, self.processed_domains_tr, self.merged_db_tr),
        executor.submit(self._merge_documents, self.adc_ct_ipb, self._merge_adc_ct_ipb, self.processed_domains_tr, self.merged_db_tr),

        executor.submit(self._merge_documents, self.ucds_gfwl, self._merge_ucds_gfwl, self.processed_domains_tr, self.comparegroup_db_tr),
        executor.submit(self._merge_documents, self.ucds_ipb, self._merge_ucds_ipb, self.processed_domains_tr, self.comparegroup_db_tr),
        executor.submit(self._merge_documents, self.bdc_cm_gfwl, self._merge_bdc_cm_gfwl, self.processed_domains_tr, self.merged_db_tr),
        executor.submit(self._merge_documents, self.bdc_ct_ipb, self._merge_bdc_ct_ipb, self.processed_domains_tr, self.merged_db_tr),
        executor.submit(self._merge_documents, self.bdc_ucds_gfwl, self._merge_bdc_ucds_gfwl, self.processed_domains_tr, self.comparegroup_db_tr),
        executor.submit(self._merge_documents, self.bdc_ucds_ipb, self._merge_bdc_ucds_ipb, self.processed_domains_tr, self.comparegroup_db_tr),
        # 2025 Data Constants
        executor.submit(self._merge_documents, self.adc_cm_dnsp_2025, self._merge_adc_cm_dnsp, self.processed_domains_dnsp, self.merged_db_2025_dns, use_dns_server=True),   # 新增
        executor.submit(self._merge_documents, self.adc_cm_gfwl_2025, self._merge_adc_cm_gfwl, self.processed_domains_tr, self.merged_db_2025_gfwl),                     # 新增
        # 2024 November Data Constants
        executor.submit(self._merge_documents, self.adc_cm_dnsp_nov, self._merge_adc_cm_dnsp_nov, self.processed_domains_dnsp, self.merged_db_2024_dns, use_dns_server=True),  # 新增
        executor.submit(self._merge_documents, self.adc_cm_gfwl_nov, self._merge_adc_cm_gfwl_nov, self.processed_domains_tr, self.merged_db_2024_gfwl),                  # 新增
      ]
      for future in concurrent.futures.as_completed(futures):
        try:
          future.result()
        except Exception as e:
          logger.error(f"Error in thread execution: {e}")
    self._finalize_documents(self.processed_domains_dnsp, self.merged_db_dnsp, is_traceroute=False, use_dns_server=True)
    self._finalize_documents(self.processed_domains_tr, self.merged_db_tr, is_traceroute=True)
    self._finalize_documents(self.processed_domains_dnsp, self.comparegroup_db_dnsp, is_traceroute=False, use_dns_server=True)
    self._finalize_documents(self.processed_domains_tr, self.comparegroup_db_tr, is_traceroute=True)
    self._finalize_documents(self.processed_domains_dnsp, self.merged_db_2025_dns, is_traceroute=False, use_dns_server=True)  # 新增
    self._finalize_documents(self.processed_domains_tr, self.merged_db_2025_gfwl, is_traceroute=True)                      # 新增
    self._finalize_documents(self.processed_domains_dnsp, self.merged_db_2024_dns, is_traceroute=False, use_dns_server=True)  # 新增
    self._finalize_documents(self.processed_domains_tr, self.merged_db_2024_gfwl, is_traceroute=True)                      # 新增

  def _merge_documents(self, db_handler, merge_function, processed_domains, target_db, use_dns_server=False):
    try:
      # 使用游标迭代而不是一次性加载所有文档
      cursor = db_handler.find({})
      for idx, document in enumerate(tqdm(cursor, desc=f"Merging {db_handler.collection.name}")):
        merge_function(document, processed_domains, use_dns_server)
    except Exception as e:
      logger.error(f"Error in _merge_documents: {e}")

  def _merge_adc_cm_dnsp(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        answers=document.get("results", []),
        dns_server=document.get("dns_server", "unknown"),
        is_traceroute=False,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_adc_cm_gfwl(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        answers=document.get("ips", []),
        error=document.get("error", []),
        mark=document.get("mark", []),
        is_traceroute=True,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_adc_ct_dnsp(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        answers=document.get("answers", []),
        dns_server=document.get("dns_server", "unknown"),
        is_traceroute=False,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_adc_ct_gfwl(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        answers=document.get("results", []),
        is_traceroute=True,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_adc_ct_ipb(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        ipv4=document.get("IPv4", []),
        ipv6=document.get("IPv6", []),
        is_accessible=document.get("is_accessible", []),
        is_traceroute=True,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_adc_cm_dnsp_nov(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        dns_server=document.get("dns_server", "unknown"),
        answers=document.get("results", []),
        error_code=document.get("error_code", []),
        error_reason=document.get("error_reason", []),
        record_type=document.get("record_type", []),
        is_traceroute=False,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_adc_cm_gfwl_nov(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        error=document.get("Error", []),
        ipv4=document.get("IPv4", []),
        ipv6=document.get("IPv6", []),
        invalid_ip=document.get("Invalid IP", []),
        rst_detected=document.get("RST Detected", []),
        redirection_detected=document.get("Redirection Detected", []),
        timestamp=document.get("timestamp", []),
        is_traceroute=True,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_ucds_dnsp(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        answers=document.get("answers", []),
        dns_server=document.get("dns_server", "unknown"),
        is_traceroute=False,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_ucds_gfwl(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        answers=document.get("results", []),
        is_traceroute=True,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_ucds_ipb(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        ipv4=document.get("IPv4", []),
        ipv6=document.get("IPv6", []),
        is_accessible=document.get("is_accessible", []),
        is_traceroute=True,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_bdc_cm_dnsp(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        answers=document.get("results", []),
        dns_server=document.get("dns_server", "unknown"),
        is_traceroute=False,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_bdc_cm_gfwl(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        answers=document.get("result", []),
        dns_server=document.get("dns_server", "unknown"),
        is_traceroute=True,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_bdc_ct_ipb(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        results_ip=document.get("results_ip", []),
        ip_type=document.get("ip_type", []),
        port=document.get("port", []),
        is_accessible=document.get("is_accessible", []),
        dns_server=document.get("dns_server", "unknown"),
        is_traceroute=True,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_bdc_ucds_dnsp(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        answers=document.get("results", []),
        dns_server=document.get("dns_server", "unknown"),
        is_traceroute=False,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_bdc_ucds_gfwl(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        answers=document.get("result", []),
        dns_server=document.get("dns_server", "unknown"),
        is_traceroute=True,
      ),
      processed_domains,
      use_dns_server
    )

  def _merge_bdc_ucds_ipb(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        results_ip=document.get("results_ip", []),
        ip_type=document.get("ip_type", []),
        port=document.get("port", []),
        is_accessible=document.get("is_accessible", []),
        dns_server=document.get("dns_server", "unknown"),
        is_traceroute=True,
      ),
      processed_domains,
      use_dns_server
    )

  def _format_document(
    self,
    domain,
    timestamp=None,
    answers=None,
    dns_server=None,
    is_traceroute=False,
    error=None,
    mark=None,
    ipv4=None,
    ipv6=None,
    invalid_ip=None,
    error_code=None,
    error_reason=None,
    record_type=None,
    rst_detected=None,
    redirection_detected=None,
    port=None,
    ip_type=None,
    is_accessible=None,
  ):
    if is_traceroute:
      return {
        "domain": domain,
        "timestamp": timestamp or [],
        "results": answers or [],
        "error": error or [],
        "mark": mark or [],
        "IPv4": ipv4 or [],
        "IPv6": ipv6 or [],
        "invalid_ip": invalid_ip or [],
        "rst_detected": rst_detected or [],
        "redirection_detected": redirection_detected or []
      }
    else:
      all_ips = set()
      pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}\b|\b[0-9a-fA-F:]+\b"
      for answer in answers:
        # 将答案拉平，并将嵌套字符串转换为列表
        if isinstance(answer, str):
          flat_values = re.findall(pattern, answer)
        elif isinstance(answer, list):
          # 修正这里以处理嵌套列表
          flat_values = set(
            chain.from_iterable(
                re.findall(pattern, str(v)) if isinstance(v, str) else [item for item in v] for v in answer
            )
          )
        else:
          flat_values = []
        # 添加调试日志以检查 `flat_values`
        logger.debug(f"Flat values: {flat_values}")
        # 拉平后加入去重集合
        all_ips.update(flat_values)
      unique_ips = list(all_ips)
      is_poisoned = False
      internal_ip_patterns = [
        r"^10\.", r"^172\.(1[6-9]|2[0-9]|3[0-1])\.", r"^192\.168\.",
        r"^127\.", r"^0\.", r"^::1", r"^fe80", r"^fc00", r"^fd00"
      ]
      for ip in unique_ips:
        for pattern in internal_ip_patterns:
          if re.match(pattern, ip):
            is_poisoned = True
            break
      return {
        "domain": self._normalize_domain(domain),
        "timestamp": timestamp or [],
        "answers": unique_ips or [],
        "dns_server": dns_server or "unknown",
        "error_code": error_code or [],
        "error_reason": error_reason or [],
        "record_type": record_type or [],
        "is_poisoned": is_poisoned or False,
      }

  def _normalize_domain(self, domain):
    # 标准化域名，移除 'www.'
    if domain.startswith("www."):
      return domain[4:]
    return domain

  def _process_document(self, document, processed_domains, use_dns_server=False):
    try:
      domain = self._normalize_domain(document["domain"])
      try:
        dns_servers = ast.literal_eval(document.get("dns_server", "['unknown']"))  # 使用 ast.literal_eval 安全地将字符串转换为列表
      except (ValueError, SyntaxError):
        dns_servers = [document.get("dns_server", "unknown")]  # 如果转换失败，则将其视为单个 DNS 服务器
      if not domain:
        logger.warning(f"Document with missing domain skipped: {document}")
        return

      timestamp = document.get("timestamp", None)  # 获取字段，None为默认值
      if isinstance(timestamp, str):  # 如果是字符串
          date = self._extract_date(timestamp)
      elif isinstance(timestamp, list) and timestamp:  # 如果是非空数组
          date = self._extract_date(timestamp[0])  # 提取第一个元素
      else:  # 其他情况
          date = "unknown"
      with self.lock:
        for dns_server in dns_servers:
          key = (date, domain, dns_server) if use_dns_server else (date, domain)
          if isinstance(timestamp, list):
            for t in timestamp:
              processed_domains[key]['timestamp'].add(t)  # 分别添加每个 timestamp
          elif isinstance(timestamp, str):
            processed_domains[key]['timestamp'].add(timestamp)  # 直接添加字符串 timestamp
          else:
            processed_domains[key]['timestamp'].add("unknown")  # 添加默认值
          for field, value in document.items():
            if field not in ["domain", "dns_server", "timestamp"]:
              if isinstance(value, list):
                flat_values = set(
                  chain.from_iterable(
                    v if isinstance(v, list) else [v] for v in value
                  )
                )
                flat_values = {v for v in flat_values if v}  # 移除空值
                processed_domains[key][field].update(flat_values)
              else:
                if value:  # 仅添加非空值
                  processed_domains[key][field].add(value)
    except Exception as e:
      logger.error(f"Error processing document: {document}, {e}")

  def _extract_date(self, timestamp):
    try:
      if isinstance(timestamp, datetime.datetime):  # 如果是 datetime 对象
        timestamp = timestamp.isoformat()  # 转为 ISO 格式字符串
      elif not isinstance(timestamp, str):  # 如果不是字符串，直接返回未知
        logger.warning(f"Unexpected timestamp type: {type(timestamp)}")
        return "unknown"
      date = timestamp.split("T")[0]  # 提取日期部分
      logger.debug(f"Extracted date: {date} from timestamp: {timestamp}")
      return date
    except Exception as e:
      logger.error(f"Error extracting date from timestamp '{timestamp}': {e}")
      return "unknown"

  def _finalize_documents(self, processed_domains, target_db, is_traceroute=False, use_dns_server=False):
    batch = []
    counter = 0  # 自增数字
    # 将 error_code_data 转换为字典，以域名作为键
    error_code_documents = self.error_domain_dsp_adc_cm.find({})
    error_code_data = {doc['domain']: doc for doc in error_code_documents if 'domain' in doc}

    for key, data in processed_domains.items():
      if use_dns_server:
        date, domain, dns_server = key
      else:
        date, domain = key
        dns_server = None

      timestamps = data.get("timestamp", [])
      for timestamp in timestamps:
        if use_dns_server:
          if target_db.collection.name not in ["2025_GFWL", "2024_Nov_GFWL", "2025_DNS", "2024_Nov_DNS"]:
            if is_traceroute:
              finalized_document = {
                # "_id": f"TRACEROUTE-{target_db.collection.name}-{is_traceroute}-{domain}-{dns_server}-{counter}",
                "domain": domain,
                "timestamp": timestamp,
                "ips": list(data.get("answers", [])) + list(data.get("IPv4", [])) + list(data.get("IPv6", [])),
                "error": list(data.get("error", [])),
                "error_reason": list(data.get("Error Reason", [])),
                "mark": list(data.get("mark", [])),
                "results": list(data.get("results", [])),
                "is_accessible": list(data.get("is_accessible", [])),
              }
              # 检查是否包含内网地址
              if '127.0.0.1' in data.get('IPv4', []) or '::1' in data.get('IPv6', []):
                  finalized_document['error'].append('Blocked')
                  finalized_document['error_reason'].append('Internal IP Address Blocked')
              # 检查特定错误信息
              for error in data.get('results', []):
                if error == 'Traceroute timed out':
                  finalized_document['error'].append('Timeout')
                elif error == 'No Answer':
                  finalized_document['error'].append('NoAnswer')
                elif error == 'Traceroute Failed':
                  finalized_document['error'].append('Failed')
                elif error == 'Not Found':
                  finalized_document['error'].append('NotFound')
                elif error == 'Network Unreachable':
                  finalized_document['error'].append('NetworkUnreachable')
                elif error == 'Host Unreachable':
                  finalized_document['error'].append('HostUnreachable')
                elif error == 'Protocol Unreachable':
                  finalized_document['error'].append('ProtocolUnreachable')
                elif error == 'Port Unreachable':
                  finalized_document['error'].append('PortUnreachable')
                elif error == 'Fragmentation Needed':
                  finalized_document['error'].append('FragmentationNeeded')
                elif error == 'Source Route Failed':
                  finalized_document['error'].append('SourceRouteFailed')
                elif error == 'Destination Network Unknown':
                  finalized_document['error'].append('DestinationNetworkUnknown')
                elif error == 'Destination Host Unknown':
                  finalized_document['error'].append('DestinationHostUnknown')
                elif error == 'Source Host Isolated':
                  finalized_document['error'].append('SourceHostIsolated')
                elif error == 'Communication with Destination Network Administratively Prohibited':
                  finalized_document['error'].append('CommunicationWithDestinationNetworkAdministrativelyProhibited')
                elif error == 'Communication with Destination Host Administratively Prohibited':
                  finalized_document['error'].append('CommunicationWithDestinationHostAdministrativelyProhibited')
                elif error == 'Destination Network Unreachable for Type of Service':
                  finalized_document['error'].append('DestinationNetworkUnreachableForTypeOfService')
                elif error == 'Destination Host Unreachable for Type of Service':
                  finalized_document['error'].append('DestinationHostUnreachableForTypeOfService')
                elif error == 'Communication Administratively Prohibited':
                  finalized_document['error'].append('CommunicationAdministrativelyProhibited')
                elif error == 'Host Precedence Violation':
                  finalized_document['error'].append('HostPrecedenceViolation')
                elif error == 'Precedence cutoff in effect':
                  finalized_document['error'].append('PrecedenceCutoffInEffect')
            else:
              finalized_document = {
                # "_id": f"DNSPOISON-{target_db.collection.name}-{is_traceroute}-{domain}-{dns_server}-{counter}",
                "domain": domain,
                "dns_server": dns_server,
                "answers": list(data["answers"]),
                "error_code": list(data["error_code"]),
                "error_reason": list(data["error_reason"]),
                "record_type": list(data["record_type"]),
                "timestamp": timestamp,
                "is_poisoned": bool(data["is_poisoned"]),
              }
          else:
            # ...existing code for specific collections...
            finalized_document = {
              # "_id": f"DNSPOISON-{target_db.collection.name}-{is_traceroute}-{domain}-{dns_server}-{counter}",
              "domain": domain,
              "dns_server": dns_server,
              "answers": list(data["answers"]),
              "error_code": list(data["error_code"]),
              "error_reason": list(data["error_reason"]),
              "record_type": list(data["record_type"]),
              "timestamp": timestamp,
              "is_poisoned": bool(data["is_poisoned"]),
            }
        else:
          # Handle documents without dns_server
          finalized_document = {
            # "_id": f"DNSPOISON-{target_db.collection.name}-{is_traceroute}-{domain}-{counter}",
            "domain": domain,
            "answers": list(data["answers"]),
            "error_code": list(data["error_code"]),
            "error_reason": list(data["error_reason"]),
            "record_type": list(data["record_type"]),
            "timestamp": timestamp,
            "is_poisoned": bool(data["is_poisoned"]),
          }

        for field, value in data.items():
          if field not in finalized_document and field != "timestamp":
            finalized_document[field] = list(value)

        # 移除 '_id' 字段以避免重复键错误
        finalized_document.pop("_id", None)

        batch.append(finalized_document)
        counter += 1  # 自增数字增加

        if domain in error_code_data:
          error_info = error_code_data[domain]
          finalized_document["error_code"] = error_info.get("error_code", [])
          finalized_document["error_reason"] = error_info.get("error_reason", [])

        if len(batch) >= BATCH_SIZE:
          self._insert_documents(batch, target_db)
          logger.info(f"Inserted batch of {len(batch)} documents into {target_db.collection.name}")
          batch = []

      if batch:
        self._insert_documents(batch, target_db)
      # 确保及时清理不需要的数据
      del data

  def _insert_documents(self, batch, target_db):
    try:
      if batch:
        target_db.insert_many(batch, ordered=False)  # 已添加 ordered=False
    except pymongo.errors.BulkWriteError as e:
      for error in e.details.get('writeErrors', []):
        if error.get('code') != 11000:
          logger.error(f"Error inserting document: {error}")
    except Exception as e:
      logger.error(f"Error inserting documents: {e}")


if __name__ == "__main__":
  try:
    logger.info("Starting DNSPoisoningMerger")
    Merged_db_DNSP.collection.drop()
    Merged_db_TR.collection.drop()
    Merged_db_2025_DNS.collection.drop()      # 新增
    Merged_db_2025_GFWL.collection.drop()     # 新增
    Merged_db_2024_DNS.collection.drop()      # 新增
    Merged_db_2024_GFWL.collection.drop()     # 新增
    CompareGroup_db_DNSP.collection.drop()
    CompareGroup_db_TR.collection.drop()
    logger.info("Merged and CompareGroup collections cleared")
    merger = Merger(
      ADC_CM_DNSP,
      ADC_CM_GFWL,
      ADC_CT_DNSP,
      ADC_CT_GFWL,
      ADC_CT_IPB,
      ADC_CM_DNSP_NOV,
      ADC_CM_GFWL_NOV,
      ERROR_DOMAIN_DSP_ADC_CM,
      UCDS_DNSP,
      UCDS_GFWL,
      UCDS_IPB,
      BDC_CM_DNSP,
      BDC_CM_GFWL,
      BDC_CT_IPB,
      BDC_UCDS_DNSP,
      BDC_UCDS_GFWL,
      BDC_UCDS_IPB,
      Merged_db_DNSP,
      Merged_db_TR,
      CompareGroup_db_DNSP,
      CompareGroup_db_TR,
      ADC_CM_DNSP_2025,
      ADC_CM_GFWL_2025,
      Merged_db_2025_DNS,   # 新增
      Merged_db_2025_GFWL,  # 新增
      Merged_db_2024_DNS,   # 新增
      Merged_db_2024_GFWL,  # 新增
    )
    merger.merge_documents()
    logger.info("DNSPoisoningMerger completed")
  except Exception as e:
    logger.error(f"Error in DNSPoisoningMerger: {e}")
