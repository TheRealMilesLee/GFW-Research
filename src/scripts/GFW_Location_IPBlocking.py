import concurrent.futures
import csv
import os
import re
import socket
import subprocess
import time
from datetime import datetime, timedelta
from urllib.request import urlretrieve

import geoip2.database
from scapy.all import sr1, IP, TCP

def get_domains_list() -> list:
  print("Reading domains list from CSV file")
  csv_file = "D:\\Developer\\GFW-Research\\src\\Import\\domains_list.csv"
  domains = []
  with open(csv_file, 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row
    for row in reader:
      domains.append(row[0])
  return domains

def download_geoip_database() -> None:
  GEOIP_DB_PATH = os.path.join(os.path.dirname(__file__), "../Import/GeoLite2-City.mmdb")
  if not os.path.exists(GEOIP_DB_PATH):
    print("Downloading GeoLite2 City database")
    urlretrieve("https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb", GEOIP_DB_PATH)

def check_domain_exists(domain: str) -> bool:
  try:
    print(f"Checking if {domain} exists")
    ip_address = socket.gethostbyname(domain)
    return True
  except socket.gaierror:
    return False

def traceroute(domain: str, use_ipv6: bool = False) -> dict:
  if use_ipv6:
    print(f"Tracerouting to {domain} using IPv6")
    command = ['tracert', '-6', domain]
  else:
    print(f"Tracerouting to {domain} using IPv4")
    command = ['tracert', '-4', domain]

  try:
    print(f"Running traceroute command: {' '.join(command)}")
    output = subprocess.check_output(command, stderr=subprocess.STDOUT, encoding='utf-8', timeout=300, errors='ignore')
    lines = output.split('\n')

    ipv4_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
    ipv6_pattern = r'([0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){7})'

    ips = {
      "ipv4": [],
      "ipv6": []
    }

    for line in lines:
      found_ipv4 = re.findall(ipv4_pattern, line)
      found_ipv6 = re.findall(ipv6_pattern, line)
      if found_ipv4:
        ips["ipv4"].extend(found_ipv4)
      if found_ipv6 and use_ipv6:
        ips["ipv6"].extend(found_ipv6)

    if use_ipv6 and not ips["ipv4"]:
      ipv4_ips = traceroute(domain, use_ipv6=False)

    print(f"Checking for TCP RST and redirection for {domain}")
    rst_detected = False
    redirection_detected = False

    for ip in ips["ipv4"]:
      pkt = IP(dst=ip) / TCP(dport=80, flags="S")
      resp = sr1(pkt, timeout=2, verbose=0)
      if resp and resp.haslayer(TCP):
        if resp[TCP].flags == "RA":
          rst_detected = True
        if resp[TCP].flags == "SA" and resp[IP].src != ip:
          redirection_detected = True

    return {"ips": ips, "rst_detected": rst_detected, "redirection_detected": redirection_detected}

  except subprocess.CalledProcessError as e:
    print(f"Error running traceroute for {domain}: {e}")
    return {"ips": {"ipv4": [], "ipv6": []}, "rst_detected": False, "redirection_detected": False}
  except subprocess.TimeoutExpired:
    print(f"Traceroute command timed out for {domain}")
    return {"ips": {"ipv4": [], "ipv6": []}, "rst_detected": False, "redirection_detected": False}

def check_domain_ipv6_support(domain: str) -> bool:
  try:
    print(f"Checking if {domain} supports IPv6")
    ipv6_address = socket.getaddrinfo(domain, None, socket.AF_INET6)
    if (ipv6_address[0][3] == ''):
      print(f"{domain} does not support IPv6")
      return False
    else:
      print(f"{domain} supports IPv6")
      return True
  except socket.gaierror:
    return False

def lookup_ip(ip: str) -> str:
  try:
    print(f"Looking up IP address {ip}")
    with geoip2.database.Reader("../Import/GeoLite2-City.mmdb") as reader:
      response = reader.city(ip)
      country = response.country.name
      region = response.subdivisions.most_specific.name if response.subdivisions else "Unknown"
      city = response.city.name if response.city.name else "Unknown"
      return f"{country}, {region}, {city}"
  except geoip2.errors.AddressNotFoundError:
    return "IP address not found in local database"
  except Exception as e:
    return f"Local IP lookup failed: {str(e)}"

def ip_lookup(ips: dict) -> dict:
  ipv6 = ips["ipv6"]
  ipv4 = ips["ipv4"]
  result = {}
  if ipv6:
    ipv6_location = lookup_ip(ipv6[0])
    result["ipv6 address"] = ipv6_location
  if ipv4:
    ipv4_location = lookup_ip(ipv4[0])
    result["ipv4 address"] = ipv4_location

  if not result:
    return {"error": "No IP addresses available for lookup"}
  return result

def process_domain(domain: str) -> dict:
  exist = check_domain_exists(domain)
  if exist:
    result = check_domain_ipv6_support(domain)
    if result:
      traceroute_output = traceroute(domain, use_ipv6=True)
    else:
      traceroute_output = traceroute(domain, use_ipv6=False)
    ips = traceroute_output["ips"]
    rst_detected = traceroute_output["rst_detected"]
    redirection_detected = traceroute_output["redirection_detected"]
    return {
      "domain": domain,
      "ips": ips,
      "location": ip_lookup(ips),
      "rst_detected": rst_detected,
      "redirection_detected": redirection_detected
    }
  else:
    try:
      destination_ip = socket.gethostbyname(domain)
    except socket.gaierror:
      return {"domain": domain, "error": "Unable to resolve domain"}
    if destination_ip in ips["ipv4"] or destination_ip in ips["ipv6"]:
      return {"domain": domain, "result": f"No GFW detected (Reached destination {destination_ip})"}
    else:
      location = ip_lookup(ips)
      return {
        "domain": domain,
        "ips": ips,
        "location": location
      }

def process_domains_concurrently(domains: list) -> list:
  print("Processing domains concurrently")
  results = []
  max_workers = os.cpu_count() or 1  # Use the number of CPUs available
  with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(0, len(domains), max_workers):
      batch = domains[i:i + max_workers]
      futures = [executor.submit(process_domain, domain) for domain in batch]
      for future in concurrent.futures.as_completed(futures):
        results.append(future.result())
  return results

def save_to_file(results: list, date_str: str) -> None:
  filename = f'GFW_Location_results_{date_str}.csv'
  folder_path = "D:\\Developer\\GFW-Research\\src\\Lib\\Data-2024-11-12\\China-Mobile\\GFWLocation"
  os.makedirs(folder_path, exist_ok=True)
  filepath = os.path.join(folder_path, filename)
  print(f"Saving results to file at {filepath}")
  with open(filepath, "a", newline='') as f:  # Open file in append mode
    writer = csv.writer(f)
    if f.tell() == 0:  # Check if file is empty to write header
      writer.writerow(["Domain", "IPv4", "IPv6", "Location", "RST Detected", "Redirection Detected", "Error"])
    for result in results:
      writer.writerow([
        result.get("domain", ""),
        ", ".join(result.get("ips", {}).get("ipv4", [])),
        ", ".join(result.get("ips", {}).get("ipv6", [])),
        result.get("location", ""),
        result.get("rst_detected", ""),
        result.get("redirection_detected", ""),
        result.get("error", "")
      ])

if __name__ == "__main__":
  start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
  end_time = start_time + timedelta(days=7)
  download_geoip_database()

  all_results = []
  date_str = datetime.now().strftime("%Y%m%d")

  while datetime.now() < end_time:
    domains = get_domains_list()
    results = process_domains_concurrently(domains)
    all_results.extend(results)

    if len(all_results) >= 2500:
      save_to_file(all_results, date_str)
      all_results = []

    current_date_str = datetime.now().strftime("%Y%m%d")
    if current_date_str != date_str:
      date_str = current_date_str

    print("Results saved to file at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    time.sleep(3600)  # Wait for 1 hour before next check

  if all_results:
    save_to_file(all_results, date_str)
