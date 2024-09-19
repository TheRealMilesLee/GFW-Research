"""
@brief This file is to check the DNS poisoning of a list of domains on multiple DNS servers.
@author: Hengyi Li
@date: 2024-09-12
@Copyright: Hengyi Li @2024 All Rights Reserved.
"""
import concurrent.futures
import csv
import os
import socket
import time
from datetime import datetime, timedelta

import dns.resolver

from Helper.get_dns_servers import get_dns_servers

# Timeout for connection attempts (in seconds) Current set to 60 seconds for slow connections networks or multi-hop connections
TIMEOUT = 60

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
  file_path = os.path.join(os.path.dirname(__file__), './db/domains_list.csv')
  results = []
  timestamp = datetime.now().isoformat()

  # Query DNS servers concurrently
  with concurrent.futures.ThreadPoolExecutor(max_workers=len(domains)/2) as executor:
    with open(file_path, 'r') as file:
      reader = csv.reader(file)
      domains = []
      for i, row in enumerate(reader):
        domains.append(row[0].strip())

    # Query each domain on each DNS server
    futures = []
    for domain in domains:
      for dns_server in dns_servers:
        future = executor.submit(query_dns, domain, dns_server)
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
def query_dns(domain: str, dns_server: str) -> dict:
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
    resolver.nameservers = [dns_server]
    resolver.timeout = TIMEOUT  # Set timeout to 120 seconds
    resolver.lifetime = TIMEOUT  # Set lifetime to 120 seconds
    ipv4_answers = []
    ipv6_answers = []

    # Query on IPv4
    try:
      ipv4_answers = resolver.resolve(domain, 'A')
    except dns.resolver.NoAnswer:
      # If no IPv4 records are found, Output to IPV4NOTFOUND.txt
      with open("./Error/IPV4NOTFOUND.txt", "a") as f:
        f.write(f"No IPv4 records found for {domain} on {dns_server}\n")
      ipv4_answers = []

    # Query on IPv6
    try:
      ipv6_answers = resolver.resolve(domain, 'AAAA')
    except dns.resolver.NoAnswer:
      # If no IPv6 records are found, Output to IPV6NOTFOUND.txt
      with open("./Error/IPV6NOTFOUND.txt", "a") as f:
        f.write(f"No IPv6 records found for {domain} on {dns_server}\n")
      ipv6_answers = []

    return {
      'domain': domain,
      'dns_server': dns_server,
      'ipv4': [answer.to_text() for answer in ipv4_answers],
      'ipv6': [answer.to_text() for answer in ipv6_answers]
      }
  except Exception as e:
    print(f"Error resolving {domain} on {dns_server}: {e}")
    return {
      'domain': domain,
      'dns_server': dns_server,
      'ipv4': [],
      'ipv6': []
    }


def save_results(results: dict) -> None:
  """
  @brief: Save the results of the DNS checks to a CSV file.
  @param results: A list of dictionaries containing the results of the DNS checks.
  @type results: list
  @return: None
  """
  # File formate as DNS_Checking_Result_YYYY_MM_DD_HH_MM.csv
  filename = f'DNS_Checking_Result_{datetime.now().strftime("%Y_%m_%d_%H_%M")}.csv'

  # Create a folder to store the results, modify the path to switch to another carrier provider
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

def ip_accessable_check(results: dict) -> list:
  """
  @brief: Check the accessibility of IP addresses for given domains.

  @param results: A dictionary containing the results of domain lookups.
  @return: A list of dictionaries containing the results of IP accessibility checks.
  """
  timestamp = datetime.now().isoformat()
  ip_check_results = []
  with concurrent.futures.ThreadPoolExecutor(max_workers=len(results)/2) as executor:
    for result in results:
      domain = result['domain']
      for ip_type in ['ipv4', 'ipv6']:
        for ip in result[f'result_{ip_type}']:
          ports_to_check = ['80', '443']
          print(f"Checking {ip} for domain {domain} with ports {ports_to_check}")
          futures = [executor.submit(check_ip, ip, int(port)) for port in ports_to_check]
          for index, future in enumerate(futures):
            is_accessible = future.result()
            ip_check_results.append({
              'timestamp': timestamp,
              'domain': domain,
              'ip': ip,
              'ip_type': ip_type,
              'port': ports_to_check[index],
              'is_accessible': is_accessible
            })
  print(f"IP blocking check completed at {datetime.now()}")
  return ip_check_results

def check_ip(ip: str, port: int) -> bool:
  """
  @brief: Check if a connection can be established to the specified IP address and port.

  @param ip: The IP address to connect to.
  @param port: The port number to connect to.
  @type ip: str
  @type port: int
  @return: True if the connection is successful, False otherwise.
  """
  try:
    family = socket.AF_INET6 if ':' in ip else socket.AF_INET
    with socket.create_connection((ip, port), timeout=TIMEOUT) as sock:
      print(f"Connection to {ip} on port {port} successful")
      return True
  except (socket.timeout, socket.error):
    print(f"Connection to {ip} on port {port} failed")
    return False

def save_ip_check_results(ip_check_results: list) -> None:
  """
  @brief: Save the IP check results to a CSV file.

  @param ip_check_results: A list of dictionaries containing the IP check results.
  @type ip_check_results: list
  @return: None
  """
  filename = f'IP_Checking_Reslt_{datetime.now().strftime("%Y_%m_%d_%H_%M")}.csv'
  # Create a folder to store the results, modify the path to switch to another carrier provider
  folder_path = '../Data/AfterDomainChange/China-Mobile/IPBlocking'
  os.makedirs(folder_path, exist_ok=True)
  filepath = os.path.join(folder_path, filename)

  with open(filepath, 'w', newline='') as csvfile:
    fieldnames = ['timestamp', 'domain', 'ip', 'ip_type', 'port', 'is_accessible']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in ip_check_results:
      writer.writerow(row)

if __name__ == "__main__":
  start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
  end_time = start_time + timedelta(days=7)

  while datetime.now() < end_time:
    results = check_poisoning()
    ip_check_results = ip_accessable_check(results)
    save_results(results)
    save_ip_check_results(ip_check_results)
    print(f"Check completed at {datetime.now()}")
    time.sleep(3600)  # Wait for 1 hour before next check
