import csv
import os
import platform
import time
from datetime import datetime

import dns.resolver
import concurrent.futures

# DNS servers to query
dns_servers = {
  'china': [
    '114.114.114.114',  # 114DNS
    '114.114.115.115',  # 114 Alternative
    '223.5.5.5',        # AliDNS
    '223.6.6.6',        # AliDNS Alternative
    '119.29.29.29',     # DNSPod
    '180.76.76.76',     # Baidu
    '202.96.128.86',    # China Telecom
    '210.21.196.6',     # China Unicom
    '218.30.118.6',     # CUCC DNS
    '211.136.17.107',   # China Mobile
    '117.50.11.11',     # One DNS
  ],
  'global': [
    '8.8.8.8',          # Google
    '8.8.4.4',          # Google Alternative
    '1.1.1.1',          # Cloudflare
    '1.0.0.1',          # Cloudflare Alternative
    '9.9.9.9',          # Quad9
    '149.112.112.112',  # Quad9 Alternative
    '208.67.222.222',   # OpenDNS
    '208.67.220.220',   # OpenDNS Alternative
    '8.26.56.26',       # Comodo Secure DNS
    '8.20.247.20',      # Comodo Secure DNS Alternative
    '199.85.126.10',    # Norton ConnectSafe DNS
    '199.85.127.10',    # Norton ConnectSafe DNS Alternative
    '77.88.8.8',        # Yandex DNS
    '77.88.8.1',        # Yandex DNS Alternative
    '94.140.14.14',     # AdGuard DNS
    '94.140.15.15',     # AdGuard DNS Alternative
  ]
}

def query_dns(domain, dns_server):
  try:
    resolver = dns.resolver.Resolver()
    resolver.nameservers = [dns_server]
    answers = resolver.resolve(domain)
    return {
      'domain': domain,
      'dns_server': dns_server,
      'ipv4': [answer.address for answer in answers if answer.version == 4],
      'ipv6': [answer.address for answer in answers if answer.version == 6]
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
  # read domains from file
  file_path = os.path.join(os.path.dirname(__file__), 'domains_list.csv')
  results = []
  timestamp = datetime.now().isoformat()

  with concurrent.futures.ThreadPoolExecutor() as executor:
    with open(file_path, 'r') as file:
      reader = csv.reader(file)
      domains = []
      for i, row in enumerate(reader):
        if i >= 1000:
          break
        domains.append(row[0].strip())

      # Submit the query_dns function for each domain and DNS server combination
      futures = [executor.submit(query_dns, domain, dns_server) for domain in domains for dns_server in dns_servers['china'] + dns_servers['global']]

      # Collect the results as they become available
      for future in concurrent.futures.as_completed(futures):
        dns_results = future.result()
        domain = dns_results['domain']
        dns_server = dns_results['dns_server']
        is_china_dns = dns_server in dns_servers['china']
        is_poisoned_ipv4 = set(dns_results['ipv4']) != set(dns_results['ipv4'])
        is_poisoned_ipv6 = set(dns_results['ipv6']) != set(dns_results['ipv6'])
        is_poisoned = is_poisoned_ipv4 or is_poisoned_ipv6

        # Find the existing result for the domain
        existing_result = next((result for result in results if result['domain'] == domain), None)

        # If the result exists, update it with the new data
        if existing_result:
          if is_china_dns:
            existing_result['china_result_ipv4'].extend(dns_results['ipv4'])
            existing_result['china_result_ipv6'].extend(dns_results['ipv6'])
          else:
            existing_result['global_result_ipv4'].extend(dns_results['ipv4'])
            existing_result['global_result_ipv6'].extend(dns_results['ipv6'])
          existing_result['is_poisoned_ipv4'] = existing_result['is_poisoned_ipv4'] or is_poisoned_ipv4
          existing_result['is_poisoned_ipv6'] = existing_result['is_poisoned_ipv6'] or is_poisoned_ipv6
          existing_result['is_poisoned'] = existing_result['is_poisoned'] or is_poisoned
        else:
          # Create a new result entry
          results.append({
            'timestamp': timestamp,
            'domain': domain,
            'china_result_ipv4': dns_results['ipv4'] if is_china_dns else [],
            'china_result_ipv6': dns_results['ipv6'] if is_china_dns else [],
            'global_result_ipv4': dns_results['ipv4'] if not is_china_dns else [],
            'global_result_ipv6': dns_results['ipv6'] if not is_china_dns else [],
            'is_poisoned': is_poisoned,
            'is_poisoned_ipv4': is_poisoned_ipv4,
            'is_poisoned_ipv6': is_poisoned_ipv6
          })

  return results

def save_results(results):
  filename = f'DNS_poisoning_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
  filepath = ""
  if platform.system().lower() == "linux":
    folder_path = 'ExperimentResult/CompareGroup/DNSPoisoning'
    os.makedirs(folder_path, exist_ok=True)
    filepath = f"{folder_path}/{filename}"
  elif platform.system().lower() == "darwin":
    folder_path = 'ExperimentResult/Mac/DNSPoisoning'
    os.makedirs(folder_path, exist_ok=True)
    filepath = f"{folder_path}/{filename}"
  else:
    filepath = f"ExperimentResult/DNSPoisoning/{filename}"
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
