import concurrent.futures
import logging
from collections import defaultdict
from itertools import chain
from threading import Lock

from DBOperations import ADC_db, Merged_db, CompareGroup_db, MongoDBHandler

# Config Logger
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# DNSPoisoning Constants
ADC_CM_DNSP = MongoDBHandler(ADC_db['China-Mobile-DNSPoisoning'])
ERROR_DOMAIN_DSP_ADC_CM = MongoDBHandler(ADC_db['ERROR_CODES'])
ADC_CT_DNSP = MongoDBHandler(ADC_db['China-Telecom-DNSPoisoning'])
ADC_UCD_DNSP = MongoDBHandler(ADC_db['UCDavis-Server-DNSPoisoning'])
ADC_CM_DNSP_NOV = MongoDBHandler(ADC_db['ChinaMobile-DNSPoisoning-November'])
Merged_db_DNSP = MongoDBHandler(Merged_db['DNSPoisoning'])
CompareGroup_db_DNSP = MongoDBHandler(CompareGroup_db['DNSPoisoning'])

MAX_WORKERS = 32  # Increase the number of threads
BATCH_SIZE = 100  # Number of documents to insert in a single batch


class DNSPoisoningMerger:
  def __init__(self, adc_cm_dnsp_nov, adc_cm_dnsp, error_domain_dsp_adc_cm, adc_ct_dnsp, adc_ucd_dnsp, merged_db_dnsp, compare_group_db_dnsp):
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
        executor.submit(self._merge_documents, self.adc_cm_dnsp_nov, self._merge_adc_cm_dnsp_nov),
        executor.submit(self._merge_documents, self.adc_cm_dnsp, self._merge_adc_cm_dnsp),
        executor.submit(self._merge_documents, self.adc_ct_dnsp, self._merge_adc_ct_dnsp),
        executor.submit(self._merge_documents, self.adc_ucd_dnsp, self._merge_adc_ucd_dnsp),
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
      documents = db_handler.find({})
      for document in documents:
        merge_function(document)
    except Exception as e:
      logger.error(f"Error in _merge_documents: {e}")

  def _merge_adc_cm_dnsp_nov(self, document):
    self._process_document(self._format_document(
      domain=document.get('domain', ''),
      answers=document.get('answers', []),
      dns_server=document.get('dns_server', []),
      error_code=document.get('error_code', []),
      error_reason=document.get('error_reason', []),
      record_type=document.get('record_type', []),
      timestamp=document.get('timestamp', [])
    ))

  def _merge_adc_cm_dnsp(self, document):
    domain = document.get('domain', '')
    if not domain:
      logger.warning(f"Skipping document with missing domain: {document}")
      return

    for res in document.get('results', []):
      error_document = list(self.error_domain_dsp_adc_cm.find({"domain": domain}))
      if error_document:
        error_code = error_document[0].get('error_code', [])
        error_reason = error_document[0].get('error_reason', [])
        record_type = error_document[0].get('record_type', [])
      else:
        error_code = []
        error_reason = []
        record_type = []

      self._process_document(self._format_document(
        domain=domain,
        answers=res.get('result_ipv4', []) + res.get('result_ipv6', []),
        dns_server=[res.get('dns_server', '')],
        error_code=error_code,
        error_reason=error_reason,
        record_type=record_type,
        timestamp=[res.get('timestamp', '')],
      ))

  def _merge_adc_ct_dnsp(self, document):
    domain = document.get('domain', '')
    if not domain:
      logger.warning(f"Skipping document with missing domain: {document}")
      return

    for res in document.get('results', []):
      self._process_document(self._format_document(
        domain=domain,
        answers=res.get('china_result_ipv4', []) +
            res.get('china_result_ipv6', []) +
            res.get('global_result_ipv4', []) +
            res.get('global_result_ipv6', []),
        timestamp=[res.get('timestamp', '')]
      ))

  def _merge_adc_ucd_dnsp(self, document):
    self._process_document(self._format_document(
      domain=document.get('domain', ''),
      answers=document.get('china_result_ipv4', []) +
          document.get('china_result_ipv6', []) +
          document.get('global_result_ipv4', []) +
          document.get('global_result_ipv6', []),
      timestamp=[document.get('timestamp', '')]
    ))

  def _format_document(self, domain, answers, dns_server=None, timestamp=None, error_code=None, error_reason=None, record_type=None):
    return {
      "domain": domain,
      "answers": answers,
      "dns_server": dns_server or [],
      "timestamp": timestamp or [],
      "error_code": error_code or [],
      "error_reason": error_reason or [],
      "record_type": record_type or []
    }

  def _process_document(self, document):
    try:
      domain = document['domain']
      if not domain:
        logger.warning(f"Document with missing domain skipped: {document}")
        return

      with self.lock:
        for key, value in document.items():
          if key != 'domain':
            if isinstance(value, list):
              flat_values = set(chain.from_iterable(v if isinstance(v, list) else [v] for v in value))
              logger.info(f"Adding {len(flat_values)} values to {key} for domain {domain}")
              self.processed_domains[domain][key].update(flat_values)
            else:
              logger.info(f"Adding {value} to {key} for domain {domain}")
              self.processed_domains[domain][key].add(value)
    except Exception as e:
      logger.error(f"Error processing document: {document}, {e}")

  def _finalize_documents(self):
    try:
      batch = []
      for domain, data in self.processed_domains.items():
        finalized_document = {"domain": domain}
        for key, value in data.items():
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
      logger.info(f'Inserting batch of {len(documents)} documents into MongoDB')
      compare_group_docs = [doc for doc in documents if 'UCDavis-Server-DNSPoisoning' in doc['domain']]
      merged_docs = [doc for doc in documents if 'UCDavis-Server-DNSPoisoning' not in doc['domain']]
      if compare_group_docs:
        self.compare_group_db_dnsp.insert_many(compare_group_docs)
      if merged_docs:
        self.merged_db_dnsp.insert_many(merged_docs)
    except Exception as e:
      logger.error(f"Error inserting documents: {e}")


if __name__ == '__main__':
  try:
    logger.info("Starting DNSPoisoningMerger")
    Merged_db_DNSP.collection.delete_many({})
    CompareGroup_db_DNSP.collection.delete_many({})
    logger.info("Merged collection cleared")
    merger = DNSPoisoningMerger(ADC_CM_DNSP_NOV, ADC_CM_DNSP, ERROR_DOMAIN_DSP_ADC_CM, ADC_CT_DNSP, ADC_UCD_DNSP, Merged_db_DNSP, CompareGroup_db_DNSP)
    logger.info("Merging documents")
    merger.merge_documents()
  except Exception as e:
    logger.error(f"Unexpected error in main: {e}")
