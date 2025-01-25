import concurrent.futures
import logging
import multiprocessing
import re
import ast
from collections import defaultdict
from itertools import chain
from threading import Lock

from .DBOperations import ADC_db, BDC_db, CompareGroup_db, Merged_db, MongoDBHandler
from tqdm import tqdm

# Config Logger
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')  # 修正格式字符串
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
MAX_WORKERS = max(CPU_CORES * 2, 256)  # Dynamically set workers
BATCH_SIZE = 10000  # Increased batch size for more efficient processing

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
    logger.info(f"Merging documents from {db_handler.collection.name}")
    try:
      documents = list(db_handler.find({}))  # Pre-load all documents into memory
      for document in tqdm(documents, desc=f"Merging {db_handler.collection.name}"):
        merge_function(document, processed_domains, use_dns_server)
    except Exception as e:
      logger.error(f"Error in _merge_documents: {e}")

  def _merge_adc_cm_dnsp(self, document, processed_domains, use_dns_server):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        ips=document.get("ips", []),
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
        ips=document.get("ips", []),
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
        ips=document.get("ips", []),
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
        ips=document.get("results", []),
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
        ips=document.get("ips", []),
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
        ips=document.get("ips", []),
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
        ips=document.get("results", []),
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
        ips=document.get("ips", []),
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
        ips=document.get("result", []),
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
        ips=document.get("ips", []),
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
        ips=document.get("result", []),
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
    ips=None,
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
        "ips": ips or [],
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
      # 更新后的IPv4和IPv6正则表达式，移除长度限制
      # Write the answer to a log file
      with open("ips.log", "a") as log_file:
        log_file.write(f"{ips}\n")
      # 将答案拉平，并将嵌套字符串转换为列表
      if isinstance(ips, str):
        # 移除方括号并解析为列表
        try:
          cleaned_answer = ast.literal_eval(ips)
          if isinstance(cleaned_answer, list):
            flat_values = [ip.strip() for ip in cleaned_answer if ip.strip()]
          else:
            flat_values = [cleaned_answer.strip()] if cleaned_answer.strip() else []
        except (ValueError, SyntaxError):
          flat_values = []

      elif isinstance(ips, list):
        flat_values = set()
        for item in ips:
          if isinstance(item, str):
            # 移除方括号并解析为列表
            try:
              cleaned_item = ast.literal_eval(item)
              if isinstance(cleaned_item, list):
                ips = [ip.strip() for ip in cleaned_item if isinstance(ip, str) and ip.strip()]
              else:
                ips = [cleaned_item.strip()] if isinstance(cleaned_item, str) and cleaned_item.strip() else []
            except (ValueError, SyntaxError):
              ips = []
            # 过滤有效的IP地址
            flat_values.update(ips)
          # ...existing code...
        all_ips.update(flat_values)
      else:
        flat_values = []
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
        "domain": domain,
        "timestamp": timestamp or [],
        "ips": unique_ips or [],
        "dns_server": dns_server or "unknown",
        "error_code": error_code or [],
        "error_reason": error_reason or [],
        "record_type": record_type or [],
        "is_poisoned": is_poisoned or False,
      }

  def _process_document(self, document, processed_domains, use_dns_server=False):
    try:
      domain = document["domain"]
      try:
        dns_servers = ast.literal_eval(document.get("dns_server", "['unknown']"))  # 使用 ast.literal_eval 安全地将字符串转换为列表
      except (ValueError, SyntaxError):
        dns_servers = [document.get("dns_server", "unknown")]  # 如果转换失败，则将其视为单个 DNS 服务器
      if not domain:
        logger.warning(f"Document with missing domain skipped: {document}")
        return

      with self.lock:
        for dns_server in dns_servers:
          key = (domain, dns_server) if use_dns_server else domain  # 根据Flag使用不同的唯一键
          for field, value in document.items():
            if field not in ["domain", "dns_server"]:
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

  def _finalize_documents(self, processed_domains, target_db, is_traceroute=False, use_dns_server=False):
    batch = []
    counter = 0  # 自增数字
    error_code_data = self.error_domain_dsp_adc_cm.find({})

    for key, data in processed_domains.items():
      if use_dns_server:
        domain, dns_server = key
        if not dns_server or len(dns_server) <= 1:
          continue  # 跳过 dns_server 为空或只有1个字符的文档
      else:
        domain = key
        dns_server = None
      if target_db.collection.name not in ["2025_GFWL", "2024_Nov_GFWL", "2025_DNS", "2024_Nov_DNS"]:
        if is_traceroute:
          finalized_document = {
            "_id": f"TRACEROUTE-{target_db.collection.name}-{is_traceroute}-{domain}-{counter}",
            "domain": domain,
            "timestamp": list(data["timestamp"]),
            "ips": list(data.get("ips", [])) + list(data.get("IPv4", [])) + list(data.get("IPv6", [])),
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
            "_id": f"DNSPOISON-{target_db.collection.name}-{is_traceroute}-{domain}-{counter}",
            "domain": domain,
            "dns_server": dns_server,
            "ips": list(data["ips"]),
            "error_code": list(data["error_code"]),
            "error_reason": list(data["error_reason"]),
            "record_type": list(data["record_type"]),
            "timestamp": list(data["timestamp"]),
            "is_poisoned": bool(data["is_poisoned"]),
          }
        for field, value in data.items():
          if field not in finalized_document:
            finalized_document[field] = list(value)
        batch.append(finalized_document)
        counter += 1  # 自增数字增加
        if domain in error_code_data:
          error_info = error_code_data[domain]
          finalized_document["error_code"] = error_info.get("error_code", [])
          finalized_document["error_reason"] = error_info.get("error_reason", [])
      else:
        if is_traceroute:
          finalized_document = {
            "_id": f"TRACEROUTE-{target_db.collection.name}-{is_traceroute}-{domain}-{counter}",
            "domain": domain,
            "timestamp": list(data["timestamp"]),
            "error": list(data.get("Error", [])),
            "error_reason": list(data.get("Error Reason", [])),
            "mark": list(data.get("mark", [])),
            "ips": list(data.get("IPv4", [])) + list(data.get("IPv6", [])),
            "invalid_ip": list(data.get("invalid_ip", [])),
            "rst_detected": list(data.get("rst_detected", [])),
            "redirection_detected": list(data.get("redirection_detected", [])),
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
            "_id": f"DNSPOISON-{target_db.collection.name}-{is_traceroute}-{domain}-{counter}",
            "domain": domain,
            "dns_server": dns_server,
            "ips": list(data["ips"]),
            "error_code": list(data["error_code"]),
            "error_reason": list(data["error_reason"]),
            "record_type": list(data["record_type"]),
            "timestamp": list(data["timestamp"]),
            "is_poisoned": bool(data["is_poisoned"]),
          }
        for field, value in data.items():
          if field not in finalized_document:
            finalized_document[field] = list(value)
        batch.append(finalized_document)
        counter += 1

      if len(batch) >= BATCH_SIZE:
        self._insert_documents(batch, target_db)
        batch = []
    if batch:
      self._insert_documents(batch, target_db)



  def _insert_documents(self, batch, target_db):
    try:
      if batch:
        logger.info(f"Inserting batch of {len(batch)} documents into {target_db.collection.name}")
        target_db.insert_many(batch)
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
