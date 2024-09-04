import csv, os, platform, socket, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import dns.resolver

def get_ips_from_domains(domains):
  ip_dict = {}
  for domain in domains:
    try:
      ipv4_answers = dns.resolver.resolve(domain, 'A')
      ipv6_answers = dns.resolver.resolve(domain, 'AAAA')
      ip_dict[domain] = {
        'ipv4': [answer.address for answer in ipv4_answers],
        'ipv6': [answer.address for answer in ipv6_answers]
      }
    except Exception as e:
      print(f"Error resolving {domain}: {e}")
      ip_dict[domain] = {'ipv4': [], 'ipv6': []}
  return ip_dict

# Timeout for connection attempts (in seconds)
TIMEOUT = 5

def check_ip(ip, port):
  try:
    # Determine the address family (IPv4 or IPv6)
    family = socket.AF_INET6 if ':' in ip else socket.AF_INET
    sock = socket.create_connection((ip, port), timeout=TIMEOUT)
    sock.close()
    print(f"Connection to {ip} on port {port} successful")
    return True
  except (socket.timeout, socket.error):
    print(f"Connection to {ip} on port {port} failed")
    return False

def run_checks():
  # read domains from file
  file_path = os.path.join(os.path.dirname(__file__), 'domains_list.csv')
  with open(file_path, 'r') as file:
    domains = [line.strip() for line in file.readlines()]
  print(f"Starting IP blocking check at {datetime.now()}")
  results = []
  timestamp = datetime.now().isoformat()

  # First, get IPs for all domains
  ip_dict = get_ips_from_domains(domains)

  # Then cleanup the IP dictionary, deleted the domains that did not resolve or have no IPs
  ip_dict = {domain: ips for domain, ips in ip_dict.items() if ips['ipv4'] or ips['ipv6']}
  # Output to the file for the domains that has problem IP or have problem on resolving IP
  if platform.system().lower() == "linux":
    folder_path = 'ExperimentResult/CompareGroup/IPBlocking'
  elif platform.system().lower() == "darwin":
    folder_path = 'ExperimentResult/Mac'
  else:
    folder_path = 'ExperimentResult/IPBlocking'
  with open(f"{folder_path}/problem_domains.txt", 'w') as f:
    for domain in domains:
      if domain not in ip_dict:
        f.write(f"{domain} did not resolve\n")
      elif not ip_dict[domain]['ipv4'] and not ip_dict[domain]['ipv6']:
        f.write(f"{domain} has no IPs\n")

  with ThreadPoolExecutor(max_workers=4) as executor:
    for domain, ips in ip_dict.items():
      print(f"Checking domain {domain}, IPs: {ips}")
      for ip_type in ['ipv4', 'ipv6']:
        for ip in ips[ip_type]:
          ports_to_check = ['80', '443']
          print(f"Checking {ip} for domain {domain} with ports {ports_to_check}")
          futures = [executor.submit(check_ip, ip, int(port)) for port in ports_to_check]
          for index, future in enumerate(futures):
            is_accessible = future.result()
            results.append({
              'timestamp': timestamp,
              'domain': domain,
              'ip': ip,
              'ip_type': ip_type,
              'port': ports_to_check[index],
              'is_accessible': is_accessible
            })

  print(f"IP blocking check completed at {datetime.now()}")
  return results

def save_results(results):
  filename = f'IP_blocking_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
  filepath = ""
  if platform.system().lower() == "linux":
    folder_path = 'ExperimentResult/CompareGroup/IPBlocking'
    os.makedirs(folder_path, exist_ok=True)
    filepath = f"{folder_path}/{filename}"
  elif platform.system().lower() == "darwin":
    folder_path = 'ExperimentResult/Mac/IPBlocking'
    os.makedirs(folder_path, exist_ok=True)
    filepath = f"{folder_path}/{filename}"
  else:
    filepath = f"ExperimentResult/IPBlocking/{filename}"
    os.makedirs('ExperimentResult/IPBlocking', exist_ok=True)

  # Remove duplicate results
  unique_results = []
  for result in results:
    if result not in unique_results:
      unique_results.append(result)

  with open(filepath, 'w', newline='') as csvfile:
    fieldnames = ['timestamp', 'domain', 'ip', 'ip_type', 'port', 'is_accessible']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in unique_results:
      writer.writerow(row)

def main():
  while True:
    results = run_checks()
    save_results(results)
    print(f"IP blocking check completed at {datetime.now()}")
    del results  # Delete the results variable to free up memory
    time.sleep(3600)  # Wait for 1 hour before next check

if __name__ == "__main__":
  main()
