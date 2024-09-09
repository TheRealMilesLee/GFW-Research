import csv
import os
import platform
import time
from datetime import datetime
from get_dns_servers import get_dns_servers, get_dns_servers_and_providers
import dns.resolver
import concurrent.futures

def get_DNS_provider(server: str) -> str:
  dns_servers_and_providers = get_dns_servers_and_providers()
  for region, servers in dns_servers_and_providers.items():
    for server, provider in servers.items():
      if server == server:
        return provider

def get_DNS_region(server: str) -> str:
  dns_servers_and_providers = get_dns_servers_and_providers()
  for region, servers in dns_servers_and_providers.items():
    for server, provider in servers.items():
      if server == server:
        return region

def query_dns(domain, dns_server):
  try:
    resolver = dns.resolver.Resolver()
    resolver.nameservers = [dns_server]

    ipv4_answers = []
    ipv6_answers = []

    try:
      ipv4_answers = resolver.resolve(domain, 'A')
    except dns.resolver.NoAnswer:
      print(f"No IPv4 records found for {domain}")

    try:
      ipv6_answers = resolver.resolve(domain, 'AAAA')
    except dns.resolver.NoAnswer:
      print(f"No IPv6 records found for {domain}")

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

def check_poisoning():
  dns_servers = get_dns_servers()
  # read domains from file
  file_path = os.path.join(os.path.dirname(__file__), 'domains_list.csv')
  results = []
  timestamp = datetime.now().isoformat()

  with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
    with open(file_path, 'r') as file:
      reader = csv.reader(file)
      domains = []
      for i, row in enumerate(reader):
        domains.append(row[0].strip())

      # Submit the query_dns function for each domain and DNS server combination
      futures = [executor.submit(query_dns, domain, dns_server) for domain in domains for dns_server in dns_servers]

      # Collect the results as they become available
      for future in concurrent.futures.as_completed(futures):
        dns_results = future.result()
        domain = dns_results['domain']
        is_poisoned_ipv4 = set(dns_results['ipv4']) != set(dns_results['ipv4'])
        is_poisoned_ipv6 = set(dns_results['ipv6']) != set(dns_results['ipv6'])
        is_poisoned = is_poisoned_ipv4 or is_poisoned_ipv6

        # Find the existing result for the domain
        existing_result = next((result for result in results if result['domain'] == domain), None)

        # If the result exists, update it with the new data
        if existing_result:
          existing_result['china_result_ipv4'].extend(dns_results['ipv4'])
          existing_result['china_result_ipv6'].extend(dns_results['ipv6'])
          existing_result['is_poisoned_ipv4'] = existing_result['is_poisoned_ipv4'] or is_poisoned_ipv4
          existing_result['is_poisoned_ipv6'] = existing_result['is_poisoned_ipv6'] or is_poisoned_ipv6
          existing_result['is_poisoned'] = existing_result['is_poisoned'] or is_poisoned
        else:
          # Create a new result entry
          results.append({
            'timestamp': timestamp,
            'domain': domain,
            'dns_servers': dns_servers,
            'china_result_ipv4': dns_results['ipv4'],
            'china_result_ipv6': dns_results['ipv6'],
            'is_poisoned': is_poisoned,
            'is_poisoned_ipv4': is_poisoned_ipv4,
            'is_poisoned_ipv6': is_poisoned_ipv6
          })

  return results

def save_results(results):
  filename = f'DNS_poisoning_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
  filepath = ""
  if platform.system().lower() == "linux":
    folder_path = 'Data/AfterDomainChange/openSUSE/DNSPoisoning'
    os.makedirs(folder_path, exist_ok=True)
    filepath = f"{folder_path}/{filename}"
  elif platform.system().lower() == "darwin":
    folder_path = 'Data/AfterDomainChange/Mac/DNSPoisoning'
    os.makedirs(folder_path, exist_ok=True)
    filepath = f"{folder_path}/{filename}"
  else:
    filepath = f"Data/AfterDomainChange/Mechrevo/DNSPoisoning/{filename}"
  with open(filepath, "w", newline="") as csvfile:
    fieldnames = [
      "timestamp",
      "domain",
      "china_result_ipv4",
      "china_result_ipv6",
      "global_result_ipv4",
      "global_result_ipv6",
      "is_poisoned",
      "is_poisoned_ipv4",
      "is_poisoned_ipv6"
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
      # Remove duplicate data before writing to file
      row['china_result_ipv4'] = list(set(row['china_result_ipv4']))
      row['china_result_ipv6'] = list(set(row['china_result_ipv6']))
      row['global_result_ipv4'] = list(set(row['global_result_ipv4']))
      row['global_result_ipv6'] = list(set(row['global_result_ipv6']))
      writer.writerow(row)

def main():
  while True:
    results = check_poisoning()
    save_results(results)
    print(f"Check completed at {datetime.now()}")
    time.sleep(3600)  # Wait for 1 hour before next check


if __name__ == "__main__":
  main()
