import asyncio
import csv
import os
from datetime import datetime

import aiohttp
import dns.asyncresolver

from Helper.get_dns_servers import get_dns_servers

# Timeout for connection attempts (in seconds)
TIMEOUT = 10

async def query_dns(domain: str, dns_server: str) -> dict:
  """
  @brief Query DNS for A and AAAA records of a domain using specified DNS servers.

  @param domain: The domain to query.
  @param dns_server: The DNS server to use for the query.

  @return: A dictionary containing the following information:
    - 'domain': The queried domain.
    - 'dns_server': The DNS server used for the query.
    - 'ipv4': A list of IPv4 addresses associated with the domain.
    - 'ipv6': A list of IPv6 addresses associated with the domain.
  """
  resolver = dns.asyncresolver.Resolver()
  resolver.nameservers = [dns_server]
  resolver.timeout = TIMEOUT  # Set timeout to 10 seconds
  resolver.lifetime = TIMEOUT * 2  # Set lifetime to 20 seconds

  ipv4_answers = []
  ipv6_answers = []

  try:
    ipv4_answers = await resolver.resolve(domain, 'A')
  except dns.resolver.NoAnswer:
    pass

  try:
    ipv6_answers = await resolver.resolve(domain, 'AAAA')
  except dns.resolver.NoAnswer:
    pass

  return {
    'domain': domain,
    'dns_server': dns_server,
    'ipv4': [answer.to_text() for answer in ipv4_answers],
    'ipv6': [answer.to_text() for answer in ipv6_answers]
  }

async def check_poisoning() -> list:
  """
  @brief Checks for DNS poisoning by querying a list of domains on multiple DNS servers.

  @return A list of dictionaries containing the results of the DNS queries.
  Each dictionary contains the following keys:
  - 'timestamp': The timestamp when the query was made.
  - 'domain': The domain being queried.
  - 'dns_server': The DNS server used for the query.
  - 'result_ipv4': The IPv4 address(es) associated with the domain.
  - 'result_ipv6': The IPv6 address(es) associated with the domain.
  """
  dns_servers = get_dns_servers()

  file_path = os.path.join(os.path.dirname(__file__), './db/domains_list.csv')
  results = []
  timestamp = datetime.now().isoformat()

  with open(file_path, 'r') as file:
    reader = csv.reader(file)
    domains = [row[0].strip() for row in reader]

  tasks = [
    query_dns(domain, dns_server)
    for domain in domains
    for dns_server in dns_servers
  ]

  for future in asyncio.as_completed(tasks):
    try:
      dns_results = await future
      results.append({
        'timestamp': timestamp,
        'domain': dns_results['domain'],
        'dns_server': dns_results['dns_server'],
        'result_ipv4': dns_results['ipv4'],
        'result_ipv6': dns_results['ipv6'],
      })
    except Exception as e:
      print(f"Error querying {dns_results['domain']} with {dns_results['dns_server']}: {e}")

  return results

def save_results(results: list) -> None:
  """
  @brief: Save the results of the DNS checks to a CSV file.
  @param results: A list of dictionaries containing the results of the DNS checks.
  @type results: list
  @return: None
  """
  filename = f'DNS_Checking_Result_{datetime.now().strftime("%Y_%m_%d_%H_%M")}.csv'
  folder_path = '../Data/AfterDomainChange/China-Mobile/DNSPoisoning'
  os.makedirs(folder_path, exist_ok=True)
  filepath = f"{folder_path}/{filename}"

  with open(filepath, "w", newline="") as csvfile:
    fieldnames = [
      "timestamp",
      "domain",
      "dns_server",
      "result_ipv4",
      "result_ipv6",
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
      writer.writerow(row)

if __name__ == "__main__":
  results = asyncio.run(check_poisoning())
  save_results(results)
  print(f"Check completed at {datetime.now()}")
