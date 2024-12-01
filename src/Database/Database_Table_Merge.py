import concurrent.futures
import logging
import multiprocessing
from collections import defaultdict
from itertools import chain
from threading import Lock

from DBOperations import ADC_db, BDC_db, Merged_db, MongoDBHandler
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

# DNSPoisoning Constants for AfterDomainChange
ADC_CM_DNSP = MongoDBHandler(ADC_db["China-Mobile-DNSPoisoning"])
ADC_CM_GFWL = MongoDBHandler(ADC_db["China-Mobile-GFWLocation"])
ADC_CT_DNSP = MongoDBHandler(ADC_db["China-Telecom-DNSPoisoning"])
ADC_CT_GFWL = MongoDBHandler(ADC_db["China-Telecom-GFWLocation"])
ADC_CT_IPB = MongoDBHandler(ADC_db["China-Telecom-IPBlocking"])
ADC_CM_DNSP_NOV = MongoDBHandler(ADC_db["ChinaMobile-DNSPoisoning-November"])
ADC_CM_GFWL_NOV = MongoDBHandler(ADC_db["ChinaMobile-GFWLocation-November"])
ERROR_DOMAIN_DSP_ADC_CM = MongoDBHandler(ADC_db["ERROR_CODES"])
UCDS_DNSP = MongoDBHandler(ADC_db["UCDavis-Server-DNSPoisoning"])
UCDS_GFWL = MongoDBHandler(ADC_db["UCDavis-Server-GFWLocation"])
UCDS_IPB = MongoDBHandler(ADC_db["UCDavis-Server-IPBlocking"])

# DNSPoisoning Constants for BeforeDomainChange
BDC_CM_DNSP = MongoDBHandler(BDC_db["China-Mobile-DNSPoisoning"])
BDC_CM_GFWL = MongoDBHandler(BDC_db["China-Mobile-GFWLocation"])
BDC_CT_IPB = MongoDBHandler(BDC_db["China-Telecom-IPBlocking"])
BDC_UCDS_DNSP = MongoDBHandler(BDC_db["UCDavis-CompareGroup-DNSPoisoning"])
BDC_UCDS_GFWL = MongoDBHandler(BDC_db["UCDavis-CompareGroup-GFWLocation"])
BDC_UCDS_IPB = MongoDBHandler(BDC_db["UCDavis-CompareGroup-IPBlocking"])

# Merged database handler
Merged_db_DNSP = MongoDBHandler(Merged_db["DNSPoisoning"])
Merged_db_TR = MongoDBHandler(Merged_db["TraceRouteResult"])

# Optimize worker count based on CPU cores
CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 64)  # Dynamically set workers
BATCH_SIZE = 500  # Increased batch size for more efficient processing

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
    self.processed_domains_dnsp = defaultdict(lambda: defaultdict(set))
    self.processed_domains_tr = defaultdict(lambda: defaultdict(set))
    self.lock = Lock()

  def merge_documents(self):
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      futures = [
        executor.submit(self._merge_documents, self.adc_cm_dnsp, self._merge_adc_cm_dnsp, self.processed_domains_dnsp),
        executor.submit(self._merge_documents, self.adc_cm_gfwl, self._merge_adc_cm_gfwl, self.processed_domains_tr),
        executor.submit(self._merge_documents, self.adc_ct_dnsp, self._merge_adc_ct_dnsp, self.processed_domains_dnsp),
        executor.submit(self._merge_documents, self.adc_ct_gfwl, self._merge_adc_ct_gfwl, self.processed_domains_tr),
        executor.submit(self._merge_documents, self.adc_ct_ipb, self._merge_adc_ct_ipb, self.processed_domains_tr),
        executor.submit(self._merge_documents, self.adc_cm_dnsp_nov, self._merge_adc_cm_dnsp_nov, self.processed_domains_dnsp),
        executor.submit(self._merge_documents, self.adc_cm_gfwl_nov, self._merge_adc_cm_gfwl_nov, self.processed_domains_tr),
        executor.submit(self._merge_documents, self.ucds_dnsp, self._merge_ucds_dnsp, self.processed_domains_dnsp),
        executor.submit(self._merge_documents, self.ucds_gfwl, self._merge_ucds_gfwl, self.processed_domains_tr),
        executor.submit(self._merge_documents, self.ucds_ipb, self._merge_ucds_ipb, self.processed_domains_tr),
        executor.submit(self._merge_documents, self.bdc_cm_dnsp, self._merge_bdc_cm_dnsp, self.processed_domains_dnsp),
        executor.submit(self._merge_documents, self.bdc_cm_gfwl, self._merge_bdc_cm_gfwl, self.processed_domains_tr),
        executor.submit(self._merge_documents, self.bdc_ct_ipb, self._merge_bdc_ct_ipb, self.processed_domains_tr),
        executor.submit(self._merge_documents, self.bdc_ucds_dnsp, self._merge_bdc_ucds_dnsp, self.processed_domains_dnsp),
        executor.submit(self._merge_documents, self.bdc_ucds_gfwl, self._merge_bdc_ucds_gfwl, self.processed_domains_tr),
        executor.submit(self._merge_documents, self.bdc_ucds_ipb, self._merge_bdc_ucds_ipb, self.processed_domains_tr),
      ]
      for future in concurrent.futures.as_completed(futures):
        try:
          future.result()
        except Exception as e:
          logger.error(f"Error in thread execution: {e}")
    self._finalize_documents(self.processed_domains_dnsp, self.merged_db_dnsp)
    self._finalize_documents(self.processed_domains_tr, self.merged_db_tr)

  def _merge_documents(self, db_handler, merge_function, processed_domains):
    logger.info(f"Merging documents from {db_handler.collection.name}")
    try:
      documents = list(db_handler.find({}))  # Pre-load all documents into memory
      for document in tqdm(documents, desc=f"Merging {db_handler.collection.name}"):
        merge_function(document, processed_domains)
    except Exception as e:
      logger.error(f"Error in _merge_documents: {e}")

  def _merge_adc_cm_dnsp(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        results=document.get("results", []),
        is_poisoned=document.get("is_poisoned", []),
      ),
      processed_domains
    )

  def _merge_adc_cm_gfwl(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        result=document.get("result", []),
      ),
      processed_domains
    )

  def _merge_adc_ct_dnsp(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        answers=document.get("answers", []),
        is_poisoned=document.get("is_poisoned", False),
      ),
      processed_domains
    )

  def _merge_adc_ct_gfwl(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        result=document.get("result", []),
      ),
      processed_domains
    )

  def _merge_adc_ct_ipb(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        results_ip=document.get("results_ip", []),
        ip_type=document.get("ip_type", []),
        port=document.get("port", []),
        is_accessible=document.get("is_accessible", []),
        problem_domain=document.get("problem_domain", False),
      ),
      processed_domains
    )

  def _merge_adc_cm_dnsp_nov(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        dns_server=document.get("dns_server", []),
        results=document.get("results", []),
      ),
      processed_domains
    )

  def _merge_adc_cm_gfwl_nov(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        results=document.get("results", []),
        error=document.get("error", []),
        answers=document.get("answers", []),
        location=document.get("location", []),
      ),
      processed_domains
    )

  def _merge_ucds_dnsp(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        answers=document.get("answers", []),
        is_poisoned=document.get("is_poisoned", False),
      ),
      processed_domains
    )

  def _merge_ucds_gfwl(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        result=document.get("result", []),
      ),
      processed_domains
    )

  def _merge_ucds_ipb(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        results_ip=document.get("results_ip", []),
        ip_type=document.get("ip_type", []),
        port=document.get("port", []),
        is_accessible=document.get("is_accessible", []),
        problem_domain=document.get("problem_domain", False),
      ),
      processed_domains
    )

  def _merge_bdc_cm_dnsp(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        results=document.get("results", []),
        is_poisoned=document.get("is_poisoned", []),
      ),
      processed_domains
    )

  def _merge_bdc_cm_gfwl(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        result=document.get("result", []),
      ),
      processed_domains
    )

  def _merge_bdc_ct_ipb(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        results_ip=document.get("results_ip", []),
        ip_type=document.get("ip_type", []),
        port=document.get("port", []),
        is_accessible=document.get("is_accessible", ""),
      ),
      processed_domains
    )

  def _merge_bdc_ucds_dnsp(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        results=document.get("results", []),
        is_poisoned=document.get("is_poisoned", []),
      ),
      processed_domains
    )

  def _merge_bdc_ucds_gfwl(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        result=document.get("result", []),
      ),
      processed_domains
    )

  def _merge_bdc_ucds_ipb(self, document, processed_domains):
    self._process_document(
      self._format_document(
        domain=document.get("domain", ""),
        timestamp=document.get("timestamp", []),
        results_ip=document.get("results_ip", []),
        ip_type=document.get("ip_type", []),
        port=document.get("port", []),
        is_accessible=document.get("is_accessible", ""),
      ),
      processed_domains
    )

  def _format_document(
    self,
    domain,
    timestamp=None,
    results=None,
    is_poisoned=None,
    result=None,
    answers=None,
    dns_server=None,
    results_ip=None,
    ip_type=None,
    port=None,
    is_accessible=None,
    problem_domain=None,
    error=None,
    location=None,
  ):
    return {
      "domain": domain,
      "timestamp": timestamp or [],
      "results": results or [],
      "is_poisoned": is_poisoned or [],
      "result": result or [],
      "answers": answers or [],
      "dns_server": dns_server or [],
      "results_ip": results_ip or [],
      "ip_type": ip_type or [],
      "port": port or [],
      "is_accessible": is_accessible or [],
      "problem_domain": problem_domain or False,
      "error": error or [],
      "location": location or [],
    }

  def _process_document(self, document, processed_domains):
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
              processed_domains[domain][key].update(flat_values)
            else:
              processed_domains[domain][key].add(value)

    except Exception as e:
      logger.error(f"Error processing document: {document}, {e}")

  def _finalize_documents(self, processed_domains, merged_db):
    try:
      batch = []

      for domain, data in processed_domains.items():
        finalized_document = {"domain": domain}
        for key, value in data.items():
          finalized_document[key] = list(filter(None, value))
          batch.append(finalized_document)

        if len(batch) >= BATCH_SIZE:
          self._insert_documents(batch, merged_db)
          batch = []

      if batch:
        self._insert_documents(batch, merged_db)
    except Exception as e:
      logger.error(f"Error finalizing documents: {e}")

  def _insert_documents(self, batch, merged_db):
    try:
      if batch:
        # Remove _id field to avoid duplicate key error
        for document in batch:
          if '_id' in document:
            del document['_id']
        logger.info(f"Inserting batch of {len(batch)} documents into {merged_db.collection.name}")
        merged_db.insert_many(batch)
    except Exception as e:
      logger.error(f"Error inserting documents: {e}")

if __name__ == "__main__":
  try:
    logger.info("Starting DNSPoisoningMerger")
    Merged_db_DNSP.collection.drop()
    Merged_db_TR.collection.drop()
    logger.info("Merged collections cleared")
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
    )
    logger.info("Merging documents")
    merger.merge_documents()
  except Exception as e:
    logger.error(f"Unexpected error in main: {e}")
