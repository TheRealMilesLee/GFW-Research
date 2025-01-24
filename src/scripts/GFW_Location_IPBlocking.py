import concurrent.futures
import csv
import os
import re
import socket
import subprocess
from datetime import datetime, timedelta
from time import sleep
from ipaddress import ip_address
from urllib.request import urlretrieve
import py7zr
import geoip2.database
from scapy.all import IP, TCP, sr1


def get_domains_list() -> list:
  print("Reading domains list from CSV file")
  csv_file = "D:\\Developer\\GFW-Research\\src\\Import\\domains_list.csv"
  domains = []
  try:
    with open(csv_file, 'r') as file:
      reader = csv.reader(file)
      next(reader)  # Skip the header row
      for row in reader:
        domains.append(row[0])
  except FileNotFoundError:
    print(f"File not found: {csv_file}")
  except Exception as e:
    print(f"Error reading domains list: {e}")
  return domains

def download_geoip_database() -> None:
  GEOIP_DB_PATH = os.path.join(os.path.dirname(__file__), "../Import/GeoLite2-City.mmdb")
  if not os.path.exists(GEOIP_DB_PATH):
    try:
      print("Downloading GeoLite2 City database")
      urlretrieve("https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb", GEOIP_DB_PATH)
    except Exception as e:
      print(f"Error downloading GeoLite2 City database: {e}")

def check_domain_exists(domain: str) -> bool:
  try:
    print(f"Checking if {domain} exists")
    ip_address = socket.gethostbyname(domain)
    return True
  except socket.gaierror:
    return False
  except Exception as e:
    print(f"Error checking domain existence: {e}")
    return False

def get_dns_servers() -> list:
  print("读取DNS服务器列表")
  csv_file = "D:\\Developer\\GFW-Research\\src\\Import\\dns_servers.csv"
  dns_servers = []
  try:
    with open(csv_file, 'r') as file:
      reader = csv.DictReader(file)
      for row in reader:
        dns_servers.append(row['IPV4'])
  except FileNotFoundError:
    print(f"文件未找到: {csv_file}")
  except Exception as e:
    print(f"读取DNS服务器列表时出错: {e}")
  return dns_servers

def traceroute(domain: str, dns_server: str, use_ipv6: bool = False) -> dict:
  print(f"使用DNS服务器 {dns_server} 进行 traceroute")
  try:
    # 使用指定DNS服务器解析域名
    resolver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    resolver.settimeout(5)
    resolver.connect((dns_server, 53))
    resolver.close()
    if use_ipv6:
      print(f"Tracerouting to {domain} using IPv6")
      command = ['tracert', '-6', domain]
    else:
      print(f"Tracerouting to {domain} using IPv4")
      command = ['tracert', '-4', domain]

    print(f"Running traceroute command: {' '.join(command)}")
    output = subprocess.check_output(command, stderr=subprocess.STDOUT, encoding='utf-8', timeout=300, errors='ignore')
    lines = output.split('\n')

    ipv4_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
    ipv6_pattern = r'([0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){7})'

    ips = {"ipv4": [], "ipv6": []}
    invalid_ips = []  # Store invalid IPs

    for line in lines:
      found_ipv4 = re.findall(ipv4_pattern, line)
      found_ipv6 = re.findall(ipv6_pattern, line)
      if found_ipv4:
        for ip in found_ipv4:
          try:
            ip_address(ip)
            ips["ipv4"].append(ip)
          except ValueError:
            invalid_ips.append(ip)
      if found_ipv6 and use_ipv6:
        ips["ipv6"].extend(found_ipv6)

    print(f"Checking for TCP RST and redirection for {domain}")
    rst_detected = False
    redirection_detected = False

    for ip in ips["ipv4"]:
      try:
        pkt = IP(dst=ip) / TCP(dport=80, flags="S")
        resp = sr1(pkt, timeout=2, verbose=0)
        if resp and resp.haslayer(TCP):
          if resp[TCP].flags == "RA":
            rst_detected = True
          if resp[TCP].flags == "SA" and resp[IP].src != ip:
            redirection_detected = True
      except Exception as e:
        print(f"Error checking TCP RST and redirection for {ip}: {e}")

    return {"ips": ips, "rst_detected": rst_detected, "redirection_detected": redirection_detected, "invalid_ips": invalid_ips}

  except subprocess.CalledProcessError as e:
    print(f"Error running traceroute for {domain}: {e}")
    return {"ips": {"ipv4": [], "ipv6": []}, "rst_detected": False, "redirection_detected": False, "invalid_ips": [], "error": str(e)}
  except subprocess.TimeoutExpired:
    print(f"Traceroute command timed out for {domain}")
    return {"ips": {"ipv4": [], "ipv6": []}, "rst_detected": False, "redirection_detected": False, "invalid_ips": [], "error": "Traceroute timed out"}
  except Exception as e:
    print(f"Error during traceroute for {domain}: {e}")
    return {"ips": {"ipv4": [], "ipv6": []}, "rst_detected": False, "redirection_detected": False, "invalid_ips": [], "error": str(e)}

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
  except Exception as e:
    print(f"Error checking IPv6 support for {domain}: {e}")
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

def process_domain(domain: str, dns_servers: list) -> list:
  results = []
  try:
    exist = check_domain_exists(domain)
    if exist:
      for dns in dns_servers:
        result = check_domain_ipv6_support(domain)
        if result:
          traceroute_output = traceroute(domain, dns, use_ipv6=True)
        else:
          traceroute_output = traceroute(domain, dns, use_ipv6=False)
        results.append({
          "domain": domain,
          "dns_server": dns,
          "ips": traceroute_output.get("ips", {}),
          "invalid_ips": traceroute_output.get("invalid_ips", []),
          "rst_detected": traceroute_output.get("rst_detected", False),
          "redirection_detected": traceroute_output.get("redirection_detected", False),
          "error": traceroute_output.get("error", "")
        })
    else:
      results.append({"domain": domain, "error": "Domain does not exist"})
  except Exception as e:
    print(f"处理域名 {domain} 时发生错误: {e}")
    results.append({"domain": domain, "error": str(e)})
  return results

def process_domains_concurrently(domains: list, dns_servers: list) -> list:
  print("Processing domains concurrently")
  results = []
  max_workers = 128
  try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
      futures = [executor.submit(process_domain, domain, dns_servers) for domain in domains]
      for future in concurrent.futures.as_completed(futures):
        results.extend(future.result())
  except Exception as e:
    print(f"Error processing domains concurrently: {e}")
  return results

def save_to_file(results: list, date_str: str) -> None:
  filename = f'GFW_Location_results_{date_str}.csv'
  folder_path = "D:\\Developer\\GFW-Research\\src\\Lib\\Data-2025-1\\China-Mobile\\GFWLocation"
  os.makedirs(folder_path, exist_ok=True)
  filepath = os.path.join(folder_path, filename)
  print(f"Saving results to file at {filepath}")
  try:
    with open(filepath, "a", newline='') as f:
      writer = csv.writer(f)
      if f.tell() == 0:  # Check if file is empty to write header
        writer.writerow(["Domain", "DNS Server", "IPv4", "IPv6", "RST Detected", "Redirection Detected", "Invalid IP", "Error"])
      for result in results:
        writer.writerow([
          result.get("domain", ""),
          result.get("dns_server", ""),
          ", ".join(result.get("ips", {}).get("ipv4", [])),
          ", ".join(result.get("ips", {}).get("ipv6", [])),
          result.get("rst_detected", ""),
          result.get("redirection_detected", ""),
          ", ".join(result.get("invalid_ips", [])),
          result.get("error", "")
        ])
  except Exception as e:
    print(f"Error saving results to file: {e}")

if __name__ == "__main__":
  try:
    start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=7)
    download_geoip_database()
    dns_servers = get_dns_servers()  # 获取DNS服务器列表

    all_results = []
    date_str = datetime.now().strftime("%Y%m%d")

    while datetime.now() < end_time:
      domains = get_domains_list()
      results = process_domains_concurrently(domains, dns_servers)
      all_results.extend(results)

      if len(all_results) >= 2500:
        save_to_file(all_results, date_str)
        all_results = []

      current_date_str = datetime.now().strftime("%Y%m%d")
      if current_date_str != date_str:
        date_str = current_date_str

      print("Results saved to file at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
      # After all domains are processed, compress the CSV file
      folder_path = '../Lib/Data-2025-1/China-Mobile/GFWLocation'
      filename = f'GFW_Location_Results_{datetime.now().strftime("%Y_%m_%d")}.csv'
      filepath = f"{folder_path}/{filename}"
      compressed_filepath = f"{folder_path}/{filename}.7z"
      try:
        with py7zr.SevenZipFile(compressed_filepath, 'w', filters=[{'id': py7zr.FILTER_LZMA2, 'preset': 9}]) as archive:
          archive.write(filepath, arcname=filename)
        print(f"CSV file compressed to {compressed_filepath}")
      except Exception as e:
        print(f"Error compressing CSV file: {e}")
      sleep(3600)  # Wait for 1 hour before the next check
    if all_results:
      save_to_file(all_results, date_str)
  except Exception as e:
    print(f"Error in main execution: {e}")
