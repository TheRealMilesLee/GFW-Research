import concurrent.futures
import csv
import socket
import time
from datetime import datetime

import dns.resolver

from DNSPoisoning_DataCollection import dns_servers, domains


def get_ips_from_domains(domains):
  ip_dict = {}
  for domain in domains:
    try:
      answers = dns.resolver.resolve(domain, 'A')
      ip_dict[domain] = [answer.address for answer in answers]
    except Exception as e:
      print(f"Error resolving {domain}: {e}")
      ip_dict[domain] = []
  return ip_dict

# Ports to check
ports_to_check = [80, 443]  # HTTP and HTTPS

# Timeout for connection attempts (in seconds)
TIMEOUT = 5

def check_ip(ip, port):
  try:
    sock = socket.create_connection((ip, port), timeout=TIMEOUT)
    sock.close()
    return True
  except (socket.timeout, socket.error):
    return False

def run_checks():
  results = []
  timestamp = datetime.now().isoformat()

  # First, get IPs for all domains
  ip_dict = get_ips_from_domains(domains)

  with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
    future_to_check = {}
    for domain, ips in ip_dict.items():
      for ip in ips:
        for port in ports_to_check:
          future = executor.submit(check_ip, ip, port)
          future_to_check[future] = (domain, ip, port)

    for future in concurrent.futures.as_completed(future_to_check):
      domain, ip, port = future_to_check[future]
      try:
        is_accessible = future.result()
      except Exception as exc:
        is_accessible = False

      results.append({
        'timestamp': timestamp,
        'domain': domain,
        'ip': ip,
        'port': port,
        'is_accessible': is_accessible
      })
  return results

def save_results(results):
  filename = f'dns_ip_blocking_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
  with open(filename, 'w', newline='') as csvfile:
    fieldnames = ['timestamp', 'domain', 'ip', 'port', 'is_accessible']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
      writer.writerow(row)

def main():
  while True:
    results = run_checks()
    save_results(results)
    print(f"IP blocking check completed at {datetime.now()}")
    time.sleep(3600)  # Wait for 1 hour before next check

if __name__ == "__main__":
  main()
