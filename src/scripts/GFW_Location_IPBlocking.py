"""
@brief: This file is to check the location of the the GFW. By checking the last IP address of the traceroute, we can determine the location of the GFW.
The location of the IP address is determined by the GeoLite2-City.mmdb database.
@output: The output is saved in the folder "../../Lib/AfterDomainChange/Carrier Provider/GFWLocation" folder with the filename "GFW_Location_results_YYYYMMDD_HHMMSS.csv".
"""
import concurrent.futures
import csv
import os
import re
import shutil
import socket
import subprocess
import time
from datetime import datetime, timedelta
from urllib.request import urlretrieve

import geoip2.database
from scapy.all import sr1, IP, TCP


def get_domains_list() -> list:
    """
    @brief Retrieves a list of domains from a CSV file.

    @return A list of domains.

    @details This function reads a CSV file containing domain information and returns a list of domains.
    """
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
  """
  @brief Downloads the GeoLite2 City database if it does not already exist.

  This function checks if the GeoLite2 City database file is present in the
  current directory. If the file is not found, it downloads the database
  from a specified URL and saves it to the current directory.

  @return None
  """
  # Constants
  GEOIP_DB_PATH = os.path.join(os.path.dirname(__file__), "../Import/GeoLite2-City.mmdb")
  if not os.path.exists(GEOIP_DB_PATH):
    print("Downloading GeoLite2 City database")
    urlretrieve("https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb", GEOIP_DB_PATH)


def check_domain_exists(domain: str) -> bool:
  """
  @brief Checks if the specified domain exists.

  This function checks if the specified domain exists by attempting to resolve
  the domain to an IP address. If the domain resolves to an IP address, the
  function returns True; otherwise, it returns False.

  @param domain The domain to check for existence.
  @return True if the domain exists, False otherwise.
  """
  try:
    print(f"Checking if {domain} exists")
    ip_address = socket.gethostbyname(domain)
    return True
  except socket.gaierror:
    return False
def traceroute(domain: str, use_ipv6: bool = False) -> list:
  """
  Executes a traceroute to the specified domain.

  @param domain The domain to trace the route to.
  @param use_ipv6 Whether to use IPv6 for the traceroute.
  @return A list of IP addresses in the route.
  @exception subprocess.CalledProcessError If the traceroute command fails.
  """
  if use_ipv6:
    print(f"Tracerouting to {domain} using IPv6")
    if shutil.which('traceroute6'):
      command = ['traceroute6', domain]
    else:
      return []  # 如果traceroute6不可用，则返回空列表
  else:
    print(f"Tracerouting to {domain} using IPv4")
    command = ['traceroute', domain]

  try:
    print(f"Running traceroute command: {' '.join(command)}")
    output = subprocess.check_output(command, stderr=subprocess.STDOUT, encoding='utf-8', timeout=300)
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
      if found_ipv6 and use_ipv6:  # 如果use_ipv6为True，则检测ipv6
        ips["ipv6"].extend(found_ipv6)
    if use_ipv6 and not ips["ipv4"]:
      # If traceroute6 has results but no IPv4 addresses found, perform traceroute with IPv4
      ipv4_ips = traceroute(domain, use_ipv6=False)
    # Detect TCP RST and redirection
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
    return ips
  except subprocess.CalledProcessError as e:
    print(f"Error running traceroute for {domain}: {e}")
    return []
  except subprocess.TimeoutExpired:
    print(f"Traceroute command timed out for {domain}")
    return []

def check_domain_ipv6_support(domain: str) -> bool:
  """
  @brief Checks if the specified domain supports IPv6.

  This function checks if the specified domain supports IPv6 by resolving
  the domain to an IPv6 address. If the domain resolves to an IPv6 address,
  the function returns True; otherwise, it returns False.

  @param domain The domain to check for IPv6 support.
  @return True if the domain supports IPv6, False otherwise.
  """
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
  """
  @brief Lookup the geographical location of an IP address using a local GeoLite2 database.

  This function attempts to find the country, region, and city associated with the given IP address
  by querying a local GeoLite2-City.mmdb database file.

  @param ip The IP address to lookup as a string.
  @return A string in the format "Country, Region, City" if the IP address is found in the database.
      Returns "IP address not found in local database" if the IP address is not found.
      Returns "Local IP lookup failed: <error_message>" if any other exception occurs.
  """
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
  """
  @brief Perform a lookup for the given IPv4 and IPv6 addresses using a GeoIP database.

  This function takes a dictionary containing IPv4 and IPv6 addresses, performs a lookup
  using the GeoLite2-City.mmdb database, and returns the geographical location of the IPs.

  @param ips A dictionary with keys "ipv4" and "ipv6", each containing a list of IP addresses.
         Example: {"ipv4": ["192.0.2.1"], "ipv6": ["2001:0db8::1"]}

  @return A dictionary with the geographical location of the first IPv4 and IPv6 addresses found.
      Example: {"ipv4 address": "Country, Region, City", "ipv6 address": "Country, Region, City"}
      If no IP addresses are available for lookup, returns {"error": "No IP addresses available for lookup"}.
      If an IP address is not found in the local database, returns "IP address not found in local database".
      If there is an error during the lookup, returns "Local IP lookup failed: <error message>".
  """
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

def process_chunk(domains_chunk):
  print(f"Processing chunk of {len(domains_chunk)} domains")
  chunk_results = []
  for domain in domains_chunk:
    exist = check_domain_exists(domain)
    if exist == True:
      result = check_domain_ipv6_support(domain)
      if result:
        traceroute_output = traceroute(domain, use_ipv6=True)
      else:
        traceroute_output = traceroute(domain, use_ipv6=False)
      ips = traceroute_output["ips"]
      rst_detected = traceroute_output["rst_detected"]
      redirection_detected = traceroute_output["redirection_detected"]
      chunk_results.append({
        "domain": domain,
        "ips": ips,
        "location": ip_lookup(ips),
        "rst_detected": rst_detected,
        "redirection_detected": redirection_detected
      })
    else:
      try:
        destination_ip = socket.gethostbyname(domain)
      except socket.gaierror:
        chunk_results.append({"domain": domain, "error": "Unable to resolve domain"})
        continue
      if destination_ip in ips["ipv4"] or destination_ip in ips["ipv6"]:
        chunk_results.append({"domain": domain, "result": f"No GFW detected (Reached destination {destination_ip})"})
      else:
        location = ip_lookup(ips)
        chunk_results.append({
          "domain": domain,
          "ips": ips,
          "location": location
        })
  return chunk_results

def process_domain() -> list:
  """
  @brief Processes a list of domains to check for IPv6 support and perform traceroute concurrently.

  This function retrieves a list of domains, splits them into chunks of 512, and processes each chunk
  concurrently using multiple threads. It checks each domain for IPv6 support, performs a traceroute
  using either IPv6 or IPv4, parses the traceroute output to extract IP addresses, and checks if the
  destination IP is reached. If the destination IP is not reached, it performs an IP lookup to determine
  the location of the IP addresses found in the traceroute.

  @return A list of dictionaries indicating the result of the domain processing. Each dictionary contains
  the domain, IP addresses found, and their geographical location.
  """
  print("Processing domains")
  domains = get_domains_list()
  chunk_size = 512
  chunks = [domains[i:i + chunk_size] for i in range(0, len(domains), chunk_size)]
  traceroute_results = []

  with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
    for future in concurrent.futures.as_completed(futures):
      traceroute_results.extend(future.result())
  return traceroute_results

def save_to_file(results: dict) -> None:
  """
  @brief Saves the given results dictionary to a file with a timestamped filename.

  This function creates a directory if it does not exist and writes the contents of the results dictionary to a text file. The filename is generated based on the current date and time.

  @param results A dictionary containing the results to be saved.

  @return None
  """
  filename = f'GFW_Location_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
  folder_path = "D:\\Developer\\GFW-Research\\src\\Lib\\Data-2024-11-12\\China-Mobile\\GFWLocation"
  os.makedirs(folder_path, exist_ok=True)
  filepath = os.path.join(folder_path, filename)
  print(f"Saving results to file at {filepath}")
  with open(filepath, "w") as f:
    for result in results:
      f.write(f"{result}\n")

if __name__ == "__main__":
  start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
  end_time = start_time + timedelta(days=7)
  download_geoip_database()

  while datetime.now() < end_time:
    results = process_domain()
    save_to_file(results)
    print("Results saved to file at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    time.sleep(3600)  # Wait for 1 hour before next check
