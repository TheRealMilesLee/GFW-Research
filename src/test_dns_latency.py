import csv
import threading

import dns.resolver

from Helper.get_dns_servers import get_dns_servers, get_dns_servers_and_providers


def get_DNS_provider(server: str) -> str:
  dns_servers_and_providers = get_dns_servers_and_providers()
  for region, servers in dns_servers_and_providers.items():
    for server, provider in servers.items():
      if server == server:
        return provider

def import_domains() -> list:
  with open('domains_list.csv', 'r') as f:
    domains = f.readlines()
    domains = [domain.strip() for domain in domains]
  return domains


def check_latency(server: str, domains: list, results: list) -> None:
  resolver = dns.resolver.Resolver()
  resolver.nameservers = [server]
  resolver.timeout = 1
  resolver.lifetime = 1

  try:
    for domain in domains:
      try:
        resolver.resolve(domain)
        print(f"Resolved {domain} on {server}")
        results.append((server, domain))
      except dns.resolver.NXDOMAIN:
        print(f"Domain {domain} does not exist on {server}")
      except dns.resolver.Timeout:
        print(f"Timeout resolving domain {domain} on {server}")
      except dns.exception.DNSException as e:
        print(f"Error resolving domain {domain} on {server}: {e}")
  except dns.exception.DNSException as e:
    print(f"Error occurred while resolving domains on {server}: {e}")

def test_dns_latency() -> None:
  dns_servers = get_dns_servers()
  domains_to_check = import_domains()
  results = []
  block_size = 16

  for i in range(0, len(domains_to_check), block_size):
    block_domains = domains_to_check[i:i+block_size]
    threads = []

    for servers in dns_servers:
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
        provider = get_DNS_provider(server)
        writer.writerow([server, latency, provider])

  with open('error.log', 'w') as f:
    for server, latency in results:
      if latency == float('inf'):
        f.write(f"Domain resolution failed for server {server}\n")

if __name__ == '__main__':
  test_dns_latency()
