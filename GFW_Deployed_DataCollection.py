import concurrent.futures
import os
import platform
import re
import subprocess
import time
import socket
import requests
from datetime import datetime

def traceroute(domain, timeout=30, max_hops=30):
  system = platform.system().lower()
  if system == "windows":
    command = ["tracert", "-h", str(max_hops), domain]
  elif system == "linux":
    # Check if we're on openSUSE
    if os.path.exists('/etc/SuSE-release') or os.path.exists('/etc/os-release'):
      with open('/etc/os-release', 'r') as f:
        if 'suse' in f.read().lower():
          command = ["tracepath", "-m", str(max_hops), domain]
        else:
          command = ["traceroute", "-m", str(max_hops), "-n", domain]
    else:
      command = ["traceroute", "-m", str(max_hops), "-n", domain]
  elif system == "darwin":
    command = ["traceroute", "-m", str(max_hops), "-n", domain]
  else:
    raise Exception("Unsupported operating system")

  try:
    output = subprocess.check_output(command, stderr=subprocess.STDOUT, timeout=timeout, text=True)
    return output
  except subprocess.TimeoutExpired:
    return "Traceroute timed out"
  except subprocess.CalledProcessError:
    return "Traceroute failed"

def parse_traceroute(output):
  lines = output.split('\n')
  last_successful_ip = None
  ip_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
  for line in lines:
    ips = re.findall(ip_pattern, line)
    if ips:
      last_successful_ip = ips[-1]
  return last_successful_ip

def ip_lookup(ip):
  retries = 0
  while retries < 30:
    try:
      response = requests.get(f"http://ip-api.com/json/{ip}")
      data = response.json()
      if data['status'] == 'success':
        return f"{data.get('country', 'Unknown')}, {data.get('regionName', 'Unknown')}, {data.get('city', 'Unknown')}"
      else:
        return "IP lookup failed"
    except:
      retries += 1
  return "IP lookup failed"

def process_domain(domain, timeout=120, max_hops=60):
  print(f"Processing {domain}...")
  traceroute_output = traceroute(domain, timeout, max_hops)

  if "timed out" in traceroute_output.lower() or "failed" in traceroute_output.lower():
    last_ip = parse_traceroute(traceroute_output)
    if last_ip:
      location = ip_lookup(last_ip)
      return f"{domain}: Possible GFW detection at {last_ip} ({location})"
    else:
      return f"{domain}: Possible GFW detection (No IP found)"

  last_ip = parse_traceroute(traceroute_output)
  if not last_ip:
    return f"{domain}: No GFW detected (Traceroute completed)"

  try:
    destination_ip = socket.gethostbyname(domain)
    if last_ip == destination_ip:
      return f"{domain}: No GFW detected (Reached destination)"
  except:
    pass

  location = ip_lookup(last_ip)
  return f"{domain}: Possible GFW detected at {last_ip} ({location})"

def main(domains, timeout=120, max_hops=60):
  results = []
  with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    future_to_domain = {executor.submit(process_domain, domain, timeout, max_hops): domain for domain in domains}
    for future in concurrent.futures.as_completed(future_to_domain):
      domain = future_to_domain[future]
      try:
        result = future.result()
        results.append(result)
        print(result)
      except Exception as exc:
        print(f"{domain} generated an exception: {exc}")

  filename = f'GFW_Location_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
  system = platform.system().lower()
  if system == "linux":
    folder_path = 'ExperimentResult/CompareGroup/GFWLocation/'
  elif system == "darwin":
    folder_path = 'ExperimentResult/Mac/GFWDeployed/'
  else:
    folder_path = "ExperimentResult/GFWLocation/"

  os.makedirs(folder_path, exist_ok=True)
  filepath = os.path.join(folder_path, filename)

  with open(filepath, "w") as f:
    for result in results:
      f.write(f"{result}\n")

if __name__ == "__main__":
  file_path = os.path.join(os.path.dirname(__file__), 'domains_list.csv')
  with open(file_path, 'r') as file:
    domains = [line.strip() for line in file]

  while True:
    main(domains, timeout=120, max_hops=60)
    print(f"Check completed at {datetime.now()}")
    time.sleep(3600)  # Wait for 1 hour before next check
