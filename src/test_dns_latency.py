import threading
import time
import logging
import csv

import dns.resolver

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
    '123.125.81.6',     # Baidu DNS
    '101.226.4.6',      # Tencent DNS
    '123.125.81.7',     # Baidu DNS Alternative
    '101.226.4.7',      # Tencent DNS Alternative
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

sorted_dns_servers = {region: sorted(servers) for region, servers in dns_servers.items()}

def get_provider(server: str) -> str:
  for region, servers in dns_servers.items():
    if server in servers:
      return region

  return 'Unknown'

def import_domains() -> list:
  with open('domains_list.csv', 'r') as f:
    domains = f.readlines()
    domains = [domain.strip() for domain in domains]
  return domains

def test_dns_latency() -> None:
  domains_to_check = import_domains()
  results = []
  block_size = 256

  for i in range(0, len(domains_to_check), block_size):
    block_domains = domains_to_check[i:i+block_size]
    threads = []

    for region, servers in dns_servers.items():
      for server in servers:
        thread = threading.Thread(target=check_latency, args=(server, block_domains, results))
        thread.start()
        threads.append(thread)

    for thread in threads:
      thread.join()

  sorted_results = sorted(results, key=lambda x: x[1])
  top_servers = sorted_results[:10]

  with open('fastest_dns_servers.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['DNS Server', 'Latency (seconds)', 'Provider'])
    for server, latency in top_servers:
      if latency != float('inf'):
        provider = get_provider(server)
        writer.writerow([server, latency, provider])

  with open('error.log', 'w') as f:
    for server, latency in results:
      if latency == float('inf'):
        f.write(f"Domain resolution failed for server {server}\n")

def check_latency(server: str, domains: list, results: list) -> None:
  resolver = dns.resolver.Resolver()
  resolver.nameservers = [server]
  resolver.timeout = 1
  resolver.lifetime = 1

  try:
    start_time = time.time()
    for domain in domains:
      resolver.resolve(domain)
      print(f"Resolved {domain} on {server}")
    end_time = time.time()
    latency = end_time - start_time
    results.append((server, latency))
  except dns.resolver.NoAnswer:
    results.append((server, float('inf')))
  except dns.resolver.NXDOMAIN:
    results.append((server, float('inf')))
  except dns.resolver.NoNameservers:
    results.append((server, float('inf')))
  except dns.resolver.Timeout:
    results.append((server, float('inf')))
  except dns.exception.DNSException as e:
    results.append((server, float('inf')))

test_dns_latency()

