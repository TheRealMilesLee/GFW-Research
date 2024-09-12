import concurrent.futures
import csv
import os
import platform
import time
from datetime import datetime, timedelta

import dns.resolver

from get_dns_servers import get_dns_servers


def check_poisoning() -> None:
  """
  @brief Checks for DNS poisoning by querying a list of domains on multiple DNS servers.

  @return A list of dictionaries containing the results of the DNS queries.
  Each dictionary contains the following keys:
  - 'timestamp': The timestamp when the query was made.
  - 'domain': The domain being queried.
  - 'dns_servers': The DNS servers used for the query.
  - 'result_ipv4': The IPv4 address(es) associated with the domain.
  - 'result_ipv6': The IPv6 address(es) associated with the domain.
  """
  # Get a list of DNS servers
  dns_servers = get_dns_servers()
  # read domains from file
  file_path = os.path.join(os.path.dirname(__file__), 'domains_list.csv')
  results = []
  timestamp = datetime.now().isoformat()

  # Query DNS servers concurrently
  with concurrent.futures.ThreadPoolExecutor() as executor:
    with open(file_path, 'r') as file:
      reader = csv.reader(file)
      domains = []
      for i, row in enumerate(reader):
        domains.append(row[0].strip())

    # Query each domain on each DNS server
    futures = []
    for domain in domains:
      future = executor.submit(query_dns, domain, dns_servers)
      futures.append(future)

      # Collect the results as they become available
      for future in concurrent.futures.as_completed(futures):
        dns_results = future.result()
        domain = dns_results['domain']
        result_dns_servers = dns_results['dns_server']

        # Create a new result entry
        results.append({
          'timestamp': timestamp,
          'domain': domain,
          'dns_servers': result_dns_servers,
          'result_ipv4': dns_results['ipv4'],
          'result_ipv6': dns_results['ipv6'],
        })
  return results

def query_dns(domain: str, dns_servers: list) -> dict:
  """
  @brief Query DNS for A and AAAA records of a domain using specified DNS servers.

  @param domain: The domain to query.
  @param dns_servers: A list of DNS servers to use for the query.

  @return: A dictionary containing the following information:
    - 'domain': The queried domain.
    - 'dns_server': The list of DNS servers used for the query.
    - 'ipv4': A list of IPv4 addresses associated with the domain.
    - 'ipv6': A list of IPv6 addresses associated with the domain.

  @throws: Any exception that occurs during the DNS resolution process.

  @note: If no IPv4 records are found, the function will output to IPV4NOTFOUND.txt.
  @note: If no IPv6 records are found, the function will output to IPV6NOTFOUND.txt.
  """
  try:
    resolver = dns.resolver.Resolver()
    resolver.nameservers = dns_servers
    ipv4_answers = []
    ipv6_answers = []

    # Query on IPv4
    try:
      ipv4_answers = resolver.resolve(domain, 'A')
    except dns.resolver.NoAnswer:
      # If no IPv4 records are found, Output to IPV4NOTFOUND.txt
      with open("IPV4NOTFOUND.txt", "a") as f:
        f.write(f"No IPv4 records found for {domain} on {', '.join(dns_servers)}\n")
      ipv4_answers = []

    # Query on IPv6
    try:
      ipv6_answers = resolver.resolve(domain, 'AAAA')
    except dns.resolver.NoAnswer:
      # If no IPv6 records are found, Output to IPV6NOTFOUND.txt
      with open("IPV6NOTFOUND.txt", "a") as f:
        f.write(f"No IPv6 records found for {domain} on {','.join(dns_servers)}\n")
      ipv6_answers = []

    return {
      'domain': domain,
      'dns_server': dns_servers,
      'ipv4': [answer.to_text() for answer in ipv4_answers],
      'ipv6': [answer.to_text() for answer in ipv6_answers]}
  except Exception as e:
    print(f"Error resolving {domain} on {dns_servers}: {e}")
    return {
      'domain': domain,
      'dns_server': dns_servers,
      'ipv4': [],
      'ipv6': []
    }


def save_results(results: dict) -> None:
  """
  Save the DNS checking results to a CSV file.

  Args:
    results (dict): A dictionary containing the DNS checking results.

  Returns:
    None
  """
  # File formate as DNS_Checking_Result_YYYY_MM_DD_HH_MM.csv
  filename = f'DNS_Checking_Result_{datetime.now().strftime("%Y_%m_%d_%H_%M")}.csv'

  # Create a folder to store the results
  folder_path = '../Data/AfterDomainChange/China-Mobile/DNSPoisoning'
  os.makedirs(folder_path, exist_ok=True)
  filepath = f"{folder_path}/{filename}"

  # Write to file
  with open(filepath, "w", newline="") as csvfile:
    fieldnames = [
      "timestamp",
      "domain",
      "dns_servers",
      "result_ipv4",
      "result_ipv6",
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
      writer.writerow(row)

if __name__ == "__main__":
  start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
  end_time = start_time + timedelta(days=7)

  while datetime.now() < end_time:
    results = check_poisoning()
    save_results(results)
    print(f"Check completed at {datetime.now()}")
    time.sleep(3600)  # Wait for 1 hour before next check
