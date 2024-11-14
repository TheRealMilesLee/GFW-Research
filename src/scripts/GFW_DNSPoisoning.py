import asyncio
import csv
import os
from datetime import datetime, timedelta

import dns.asyncresolver
from get_dns_servers import get_dns_servers

# Timeout for connection attempts (in seconds)
TIMEOUT = 30
BATCH_SIZE = 16
CONCURRENT_TASKS = 8

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

  except Exception as e:
      error_code = 'UnknownError'
      error_reason = f"Unexpected error querying {domain} on {dns_server}: {e}"


  # 直接输出 answers 结果
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

async def check_poisoning(domains: list, ipv4_dns_servers: list, ipv6_dns_servers: list) -> list:
  results = []
  semaphore = asyncio.Semaphore(CONCURRENT_TASKS)

  async def sem_query_dns(domain, dns_server, record_type):
    async with semaphore:
      return await query_dns(domain, dns_server, record_type)

  tasks = [
    sem_query_dns(domain, dns_server, 'A')
    for domain in domains
    for dns_server in ipv4_dns_servers
  ] + [
    sem_query_dns(domain, dns_server, 'AAAA')
    for domain in domains
    for dns_server in ipv6_dns_servers
  ]

  for future in asyncio.as_completed(tasks):
    try:
      dns_results = await future
      results.append(dns_results)
    except Exception as e:
      error_folder_path = '../Lib/Data-2024-11-12/China-Mobile/Error'
      os.makedirs(error_folder_path, exist_ok=True)
      error_filename = f"ErrorDomains_{datetime.now().strftime('%Y_%m_%d')}.txt"
      error_filepath = os.path.join(error_folder_path, error_filename)
      with open(error_filepath, "a") as error_file:
        error_file.write(f"Error querying domain with server: {e}\n")

  return results

def save_results(results: list) -> None:
  filename = f'DNS_Checking_Result_{datetime.now().strftime("%Y_%m_%d_%H_%M")}.csv'
  folder_path = '../Lib/Data-2024-11-12/China-Mobile/DNSPoisoning'
  os.makedirs(folder_path, exist_ok=True)
  filepath = f"{folder_path}/{filename}"

  print(f"Saving results to {filepath}")
  with open(filepath, "w", newline="") as csvfile:
    fieldnames = [
      "timestamp",
      "domain",
      "dns_server",
      "record_type",
      "answers",
      "error_code",
      "error_reason"
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
      writer.writerow({
        "timestamp": datetime.now().isoformat(),
        "domain": row['domain'],
        "dns_server": row['dns_server'],
        "record_type": row['record_type'],
        "answers": row['answers'],
        "error_code": row['error_code'],
        "error_reason": row['error_reason']
      })

async def main():
  ipv4_dns_servers, ipv6_dns_servers = get_dns_servers()
  file_path = os.path.join(os.path.dirname(__file__), 'D:\\Developer\\GFW-Research\\src\\Import\\domains_list.csv')

  with open(file_path, 'r') as file:
    reader = csv.reader(file)
    domains = [row[0].strip() for row in reader]

  print(f"Checking {len(domains)} domains for DNS poisoning")

  end_time = datetime.now() + timedelta(days=7)
  while datetime.now() < end_time:
    for i in range(0, len(domains), BATCH_SIZE):
      batch = domains[i:i + BATCH_SIZE]
      results = await check_poisoning(batch, ipv4_dns_servers, ipv6_dns_servers)
      save_results(results)
      print(f"Batch completed at {datetime.now()}")
    await asyncio.sleep(3600)  # Wait for 1 hour before the next check

if __name__ == "__main__":
  asyncio.run(main())
