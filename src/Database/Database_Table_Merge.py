import concurrent.futures
import logging
import multiprocessing
from collections import defaultdict
from itertools import chain
from threading import Lock

from DBOperations import ADC_db, Merged_db, MongoDBHandler
from tqdm import tqdm

# Config Logger
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
  handlers=[logging.StreamHandler(), logging.FileHandler('merge_performance.log')]
)
logger = logging.getLogger(__name__)
# DNSPoisoning Constants
ADC_CM_DNSP = MongoDBHandler(ADC_db["China-Mobile-DNSPoisoning"])
ERROR_DOMAIN_DSP_ADC_CM = MongoDBHandler(ADC_db["ERROR_CODES"])
ADC_CT_DNSP = MongoDBHandler(ADC_db["China-Telecom-DNSPoisoning"])
ADC_CM_DNSP_NOV = MongoDBHandler(ADC_db["ChinaMobile-DNSPoisoning-November"])
Merged_db_DNSP = MongoDBHandler(Merged_db["DNSPoisoning"])
# Optimize worker count based on CPU cores
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 32)  # Dynamically set workers
BATCH_SIZE = 500  # Increased batch size for more efficient processing


class DNSPoisoningMerger:
  def __init__(
    self,
    adc_cm_dnsp_nov,
    adc_cm_dnsp,
    error_domain_dsp_adc_cm,
    adc_ct_dnsp,
    adc_ucd_dnsp,
    merged_db_dnsp,
    compare_group_db_dnsp,
  ):
    self.adc_cm_dnsp_nov = adc_cm_dnsp_nov
    self.adc_cm_dnsp = adc_cm_dnsp
    self.error_domain_dsp_adc_cm = error_domain_dsp_adc_cm
    self.adc_ct_dnsp = adc_ct_dnsp
    self.adc_ucd_dnsp = adc_ucd_dnsp
    self.merged_db_dnsp = merged_db_dnsp
    self.compare_group_db_dnsp = compare_group_db_dnsp
    self.processed_domains = defaultdict(lambda: defaultdict(set))
    self.lock = Lock()

  def merge_documents(self):
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      futures = [
        executor.submit(
          self._merge_documents,
          self.adc_cm_dnsp_nov,
          self._merge_adc_cm_dnsp_nov,
        ),
        executor.submit(
          self._merge_documents, self.adc_cm_dnsp, self._merge_adc_cm_dnsp, compare_group=False
        ),
        executor.submit(
          self._merge_documents, self.adc_ct_dnsp, self._merge_adc_ct_dnsp, compare_group=False
        ),
        executor.submit(
          self._merge_documents, self.adc_ucd_dnsp, self._merge_adc_ucd_dnsp, compare_group=True
        ),
      ]
      for future in concurrent.futures.as_completed(futures):
        try:
          future.result()
        except Exception as e:
          logger.error(f"Error in thread execution: {e}")
    self._finalize_documents()

  def _merge_documents(self, db_handler, merge_function, compare_group=False):
    logger.info(f"Merging documents from {db_handler.collection.name}")
    try:
      documents = list(db_handler.find({}))  # Pre-load all documents into memory
      for document in tqdm(documents, desc=f"Merging {db_handler.collection.name}"):
        document["compare_group"] = compare_group
        merge_function(document)
    except Exception as e:
      logger.error(f"Error in _merge_documents: {e}")

  def _merge_adc_cm_dnsp_nov(self, document, compare_group=False):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        answers=document.get("answers", []),
        dns_server=document.get("dns_server", []),
        error_code=document.get("error_code", []),
        error_reason=document.get("error_reason", []),
        record_type=document.get("record_type", []),
        timestamp=document.get("timestamp", []),
      ),
      compare_group
    )

  def _merge_adc_cm_dnsp(self, document, compare_group=False):
    domain = document.get("domain", "")
    if not domain:
      logger.warning(f"Skipping document with missing domain: {document}")
      return

    if not hasattr(self, "_domain_cache"):
      self._domain_cache = {}

    if domain not in self._domain_cache:
      error_document = self.error_domain_dsp_adc_cm.find_one(
        {"domain": domain},
        {"error_code": 1, "error_reason": 1, "record_type": 1, "_id": 0}
      )
      self._domain_cache[domain] = error_document or {}

    cached_doc = self._domain_cache[domain]
    error_code = cached_doc.get("error_code", [])
    error_reason = cached_doc.get("error_reason", [])
    record_type = cached_doc.get("record_type", [])

    for res in document.get("results", []):
      self._process_document(
        self._format_document(
          domain=domain,
          answers=res.get("result_ipv4", []) + res.get("result_ipv6", []),
          dns_server=[res.get("dns_server", "")],
          error_code=error_code,
          error_reason=error_reason,
          record_type=record_type,
          timestamp=[res.get("timestamp", "")]
        ),
        compare_group
      )

  def _merge_adc_ct_dnsp(self, document, compare_group=False):
    domain = document.get("domain", "")
    if not domain:
      logger.warning(f"Skipping document with missing domain: {document}")
      return

    for res in document.get("results", []):
      self._process_document(
        self._format_document(
          domain=domain,
          answers=res.get("china_result_ipv4", [])
          + res.get("china_result_ipv6", [])
          + res.get("global_result_ipv4", [])
          + res.get("global_result_ipv6", []),
          timestamp=[res.get("timestamp", "")],
        ),
        compare_group
      )

  def _format_document(
    self,
    domain,
    answers,
    dns_server=None,
    timestamp=None,
    error_code=None,
    error_reason=None,
    record_type=None,
  ):
    return {
      "domain": domain,
      "answers": answers,
      "dns_server": dns_server or [],
      "timestamp": timestamp or [],
      "error_code": error_code or [],
      "error_reason": error_reason or [],
      "record_type": record_type or [],
    }

  def _process_document(self, document, compare_group=False):
    try:
      domain = document["domain"]
      if not domain:
        logger.warning(f"Document with missing domain skipped: {document}")
        return

      with self.lock:
        for key, value in document.items():
          if key != "domain":
            if isinstance(value, list):
              flat_values = set(
                chain.from_iterable(
                  v if isinstance(v, list) else [v] for v in value
                )
              )
              self.processed_domains[domain][key].update(flat_values)
            else:
              self.processed_domains[domain][key].add(value)

    except Exception as e:
      logger.error(f"Error processing document: {document}, {e}")

  def _finalize_documents(self):
    try:
      batch = []
      compare_group_batch = []
      for domain, data in self.processed_domains.items():
        finalized_document = {"domain": domain}
        for key, value in data.items():
          finalized_document[key] = list(filter(None, value))

        if finalized_document.get("compare_group", False):
          compare_group_batch.append(finalized_document)
        else:
          batch.append(finalized_document)

        if len(batch) >= BATCH_SIZE:
          self._insert_documents(batch, compare_group_batch)
          batch = []
          compare_group_batch = []

      if batch or compare_group_batch:
        self._insert_documents(batch, compare_group_batch)
    except Exception as e:
      logger.error(f"Error finalizing documents: {e}")

  def _insert_documents(self, batch, compare_group_batch):
    try:
      if batch:
        logger.info(f"Inserting batch of {len(batch)} documents into Merged MongoDB")
        self.merged_db_dnsp.insert_many(batch)
      if compare_group_batch:
        logger.info(f"Inserting batch of {len(compare_group_batch)} documents into CompareGroup MongoDB")
        self.compare_group_db_dnsp.insert_many(compare_group_batch)
    except Exception as e:
      logger.error(f"Error inserting documents: {e}")

####################################################### Merge TraceRoute Results ######################################################

# TraceRouteConstants
ADC_CT_GFWL = MongoDBHandler(ADC_db["China-Telecom-GFWLocation"])
ADC_CT_IPB = MongoDBHandler(ADC_db["China-Telecom-IPBlocking"])
ADC_CM_GFWL_NOV = MongoDBHandler(ADC_db["ChinaMobile-GFWLocation-November"])
Merged_db_TR = MongoDBHandler(Merged_db["TraceRouteResult"])
CompareGroup_db_TR = MongoDBHandler(CompareGroup_db["TraceRouteResult"])


class TraceRouteMerger:
  def __init__(
    self,
    adc_ct_gfwl,
    adc_ct_ipb,
    adc_cm_gfwl_nov,
    merged_db_tr,
    compare_group_db_tr,
  ):
    self.adc_ct_gfwl = adc_ct_gfwl
    self.adc_ct_ipb = adc_ct_ipb
    self.adc_cm_gfwl_nov = adc_cm_gfwl_nov
    self.merged_db_tr = merged_db_tr
    self.compare_group_db_tr = compare_group_db_tr
    self.processed_domains = defaultdict(lambda: defaultdict(set))
    self.lock = Lock()

  def merge_documents(self):
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      futures = [
        executor.submit(
          self._merge_documents, self.adc_ct_gfwl, self._merge_adc_ct_gfwl
        ),
        executor.submit(
          self._merge_documents, self.adc_ct_ipb, self._merge_adc_ct_ipb
        ),
        executor.submit(
          self._merge_documents, self.adc_cm_gfwl_nov, self._merge_adc_cm_gfwl_nov
        ),
      ]
      for future in concurrent.futures.as_completed(futures):
        try:
          future.result()
        except Exception as e:
          logger.error(f"Error in thread execution: {e}")
    self._finalize_documents()

  def _merge_documents(self, db_handler, merge_function):
    logger.info(f"Merging documents from {db_handler.collection.name}")
    try:
      documents = list(db_handler.find({}))  # Pre-load all documents into memory
      for document in tqdm(documents, desc=f"Merging {db_handler.collection.name}"):
        merge_function(document)
    except Exception as e:
      logger.error(f"Error in _merge_documents: {e}")

  def _merge_adc_ct_gfwl(self, document):
    domain = document.get("domain", "")
    if not domain:
      logger.warning(f"Skipping document with missing domain: {document}")
      return

    for res in document.get("results", []):
      self._process_document(
        self._format_document(
          domain=domain,
          timestamp=[res.get("timestamp", "")],
          destination_ip=[res.get("destination_ip", "")],
        )
      )

  def _merge_adc_ct_ipb(self, document):
    domain = document.get("domain", "")
    if not domain:
      logger.warning(f"Skipping document with missing domain: {document}")
      return

    for res in document.get("results", []):
      ip_type = res.get("ip_type", "")
      if ip_type == "ipv4":
        self._process_document(
          self._format_document(
            domain=domain,
            timestamp=[res.get("timestamp", "")],
            IPV4=[res.get("ip", "")],
            Port=[res.get("Port", "")],
            is_accessable=res.get("is_accessable", ""),
          )
        )
      elif ip_type == "ipv6":
        self._process_document(
          self._format_document(
            domain=domain,
            timestamp=[res.get("timestamp", "")],
            IPV6=[res.get("ip", "")],
            Port=[res.get("Port", "")],
            is_accessable=res.get("is_accessable", ""),
          )
        )

  def _merge_adc_cm_gfwl_nov(self, document):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=[document.get("timestamp", "")],
        Invalid_IP=[document.get("Invalid_IP", "")],
        RST_Detected=document.get("RST_Detected", ""),
        Redirected_detected=document.get("Redirected_detected", ""),
      )
    )

  def _format_document(
    self,
    domain,
    timestamp=None,
    IPV4=None,
    IPV6=None,
    Port=None,
    is_accessable=None,
    destination_ip=None,
    Invalid_IP=None,
    RST_Detected=None,
    Redirected_detected=None,
  ):
    return {
      "domain": domain,
      "timestamp": timestamp or [],
      "IPV4": IPV4 or [],
      "IPV6": IPV6 or [],
      "Port": Port or [],
      "is_accessable": is_accessable or "",
      "destination_ip": destination_ip or [],
      "Invalid_IP": Invalid_IP or [],
      "RST_Detected": RST_Detected or "",
      "Redirected_detected": Redirected_detected or "",
    }

  def _process_document(self, document):
    try:
      domain = document["domain"]
      if not domain:
        logger.warning(f"Document with missing domain skipped: {document}")
        return

      with self.lock:
        for key, value in document.items():
          if key != "domain":
            if isinstance(value, list):
              flat_values = set(
                chain.from_iterable(
                  v if isinstance(v, list) else [v] for v in value
                )
              )
              self.processed_domains[domain][key].update(flat_values)
            else:
              self.processed_domains[domain][key].add(value)
    except Exception as e:
      logger.error(f"Error processing document: {document}, {e}")

  def _determine_status(self, values):
    if all(v == "yes" for v in values):
      return "yes"
    elif all(v == "no" for v in values):
      return "no"
    else:
      return "sometimes"

  def _finalize_documents(self):
    try:
      batch = []
      for domain, data in self.processed_domains.items():
        finalized_document = {"domain": domain}
        for key, value in data.items():
          if key in ["is_accessable", "RST_Detected", "Redirected_detected"]:
            finalized_document[key] = self._determine_status(value)
          else:
            finalized_document[key] = list(filter(None, value))
        batch.append(finalized_document)

        if len(batch) >= BATCH_SIZE:
          self._insert_documents(batch)
          batch = []
      if batch:
        self._insert_documents(batch)
    except Exception as e:
      logger.error(f"Error finalizing documents: {e}")

  def _insert_documents(self, documents):
    try:
      logger.info(f"Inserting batch of {len(documents)} documents into MongoDB")
      self.merged_db_tr.insert_many(documents)
    except Exception as e:
      logger.error(f"Error inserting documents: {e}")


if __name__ == "__main__":
  try:
    logger.info("Starting DNSPoisoningMerger")
    Merged_db_DNSP.collection.delete_many({})
    CompareGroup_db_DNSP.collection.delete_many({})
    logger.info("Merged collection cleared")
    merger = DNSPoisoningMerger(
      ADC_CM_DNSP_NOV,
      ADC_CM_DNSP,
      ERROR_DOMAIN_DSP_ADC_CM,
      ADC_CT_DNSP,
      Merged_db_DNSP,
    )
    logger.info("Merging documents")
    merger.merge_documents()

    logger.info("Starting TraceRouteMerger")
    Merged_db_TR.collection.delete_many({})
    CompareGroup_db_TR.collection.delete_many({})
    logger.info("Merged collection cleared")
    TraceRouteMergerWorkflow = TraceRouteMerger(
      ADC_CT_GFWL, ADC_CT_IPB, ADC_CM_GFWL_NOV, Merged_db_TR, CompareGroup_db_TR
    )
    logger.info("Merging documents")
    TraceRouteMergerWorkflow.merge_documents()
  except Exception as e:
    logger.error(f"Unexpected error in main: {e}")
