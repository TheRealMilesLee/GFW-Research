import asyncio
import os
import sys
from datetime import datetime, timedelta

import dns.asyncresolver
from get_dns_servers import get_dns_servers

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from Database.DBOperations import ADC_db, MongoDBHandler

# Timeout for connection attempts (in seconds)
TIMEOUT = 30
BATCH_SIZE = 128
CONCURRENT_TASKS = 128
WRITE_THRESHOLD = 2500


async def query_dns(domain: str, dns_server: str, record_type: str) -> dict:
  resolver = dns.asyncresolver.Resolver()
  resolver.nameservers = [dns_server]
  resolver.timeout = TIMEOUT
  resolver.lifetime = TIMEOUT + 5

  answers = []
  error_code = None
  error_reason = None

  try:
    print(f"Querying {domain} on {dns_server} for {record_type}")
    answers = await resolver.resolve(domain, record_type)
  except dns.resolver.Timeout:
    error_code = 'Timeout'
    error_reason = f"Timeout occurred for domain: {domain} on server: {dns_server}"
  except dns.resolver.NoAnswer:
    error_code = 'NoAnswer'
    error_reason = f"No answer for domain: {domain} on server: {dns_server}"
  except dns.resolver.NXDOMAIN:
    error_code = 'NXDOMAIN'
    error_reason = f"Non-existent domain: {domain} on server: {dns_server}"
  except dns.resolver.YXDOMAIN:
    error_code = 'YXDOMAIN'
    error_reason = f"Domain name should not exist: {domain} on server: {dns_server}"
  except dns.resolver.NoNameservers:
    error_code = 'NoNameservers'
    error_reason = f"No nameservers for domain: {domain} on server: {dns_server}"
  except dns.resolver.ServFail:
    error_code = 'ServFail'
    error_reason = f"Server failure for domain: {domain} on server: {dns_server}"
  except dns.resolver.Refused:
    error_code = 'REFUSED'
    error_reason = f"Server refused to answer for domain: {domain} on server: {dns_server}"
  except dns.resolver.FormErr:
    error_code = 'FORMERR'
    error_reason = f"Format error for domain: {domain} on server: {dns_server}"
  except Exception as e:
    error_code = 'UnknownError'
    error_reason = f"Unexpected error querying {domain} on {dns_server}: {e}"
  else:
    for rdata in answers:
      print(f"Record: {rdata}")

  return {
      'domain': domain,
      'dns_server': dns_server,
      'record_type': record_type,
      'answers': [answer.to_text() for answer in answers],
      'error_code': error_code,
      'error_reason': error_reason
  }


async def check_poisoning(domains: list, ipv4_dns_servers: list,
                          ipv6_dns_servers: list) -> list:
  results = []
  semaphore = asyncio.Semaphore(CONCURRENT_TASKS)

  async def sem_query_dns(domain, dns_server, record_type):
    async with semaphore:
      return await query_dns(domain, dns_server, record_type)

  tasks = [
      sem_query_dns(domain, dns_server, 'A') for domain in domains
      for dns_server in ipv4_dns_servers
  ] + [
      sem_query_dns(domain, dns_server, 'AAAA') for domain in domains
      for dns_server in ipv6_dns_servers
  ]

  for future in asyncio.as_completed(tasks):
    try:
      dns_results = await future
      results.append(dns_results)
    except Exception as e:
      error_folder_path = os.path.join(
          os.path.dirname(__file__),
          '../Lib/AfterDomainChange/China-Mobile/Error')
      os.makedirs(error_folder_path, exist_ok=True)
      error_filename = f"ErrorDomains_{datetime.now().strftime('%Y_%m_%d')}.txt"
      error_filepath = os.path.join(error_folder_path, error_filename)
      with open(error_filepath, "a") as error_file:
        error_file.write(f"Error querying domain with server: {e}\n")

  return results


def save_to_mongodb(results: list, mongodb_handler: MongoDBHandler) -> None:
  """
  将 DNS 查询结果保存到 MongoDB

  Args:
    results: 要保存的结果列表
    mongodb_handler: MongoDB 处理器实例
  """
  if not results:
    print("No results to save")
    return

  print(f"Saving {len(results)} DNS results to MongoDB")
  try:
    documents = []
    for row in results:
      doc = {
          "timestamp": datetime.now(),
          "domain": row['domain'],
          "dns_server": row['dns_server'],
          "record_type": row['record_type'],
          "answers": row['answers'],
          "error_code": row['error_code'],
          "error_reason": row['error_reason']
      }
      documents.append(doc)

    if documents:
      mongodb_handler.insert_many(documents, ordered=False)
      print(
          f"Successfully saved {len(documents)} documents to MongoDB at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
      )
  except Exception as e:
    print(f"Error saving results to MongoDB: {e}")


async def main():
  try:
    ipv4_dns_servers, ipv6_dns_servers = get_dns_servers()
    file_path = os.path.join(os.path.dirname(__file__),
                             '../Import/domains_list.csv')

    with open(file_path, 'r') as file:
      lines = file.readlines()[1:]  # Skip the header row
      domains = [line.strip().split(',')[0] for line in lines]

    print(f"Checking {len(domains)} domains for DNS poisoning")

    all_results = []  # Collect all results here
    end_time = datetime.now() + timedelta(days=7)

    # 根据当前月份创建或获取 MongoDB collection
    current_month = datetime.now().strftime("%Y_%m")
    collection_name = f"DNSPoisoning_{current_month}"
    mongodb_handler = MongoDBHandler(ADC_db[collection_name])
    print(f"Using MongoDB collection: {collection_name}")

    while datetime.now() < end_time:
      for i in range(0, len(domains), BATCH_SIZE):
        batch = domains[i:i + BATCH_SIZE]
        results = await check_poisoning(batch, ipv4_dns_servers,
                                        ipv6_dns_servers)
        all_results.extend(results)  # Append batch results to all_results

        # 检查是否需要切换到新的月份集合
        new_month = datetime.now().strftime("%Y_%m")
        if new_month != current_month:
          # 保存剩余数据到旧集合
          if all_results:
            save_to_mongodb(all_results, mongodb_handler)
            all_results = []

          # 切换到新月份的集合
          current_month = new_month
          collection_name = f"DNSPoisoning_{current_month}"
          mongodb_handler = MongoDBHandler(ADC_db[collection_name])
          print(f"Switched to new MongoDB collection: {collection_name}")

        if len(all_results) >= WRITE_THRESHOLD:
          save_to_mongodb(all_results, mongodb_handler)
          all_results.clear(
          )  # Clear results after saving to prepare for the next batch

      print(f"All batches completed at {datetime.now()}")

      # Save remaining results to MongoDB after all batches are processed
      if all_results:
        save_to_mongodb(all_results, mongodb_handler)
        all_results.clear()  # Clear results after saving

      await asyncio.sleep(3600)  # Wait for 1 hour before the next check

  except Exception as e:
    print(f"Error in main execution: {e}")


if __name__ == "__main__":
  asyncio.run(main())
