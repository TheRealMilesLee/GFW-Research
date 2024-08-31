import subprocess
import re
import requests
import concurrent.futures
import platform
from DNSPoisoning_DataCollection import domains
from datetime import datetime

def traceroute(domain, timeout=30, max_hops=30):
  command = "tracert" if platform.system().lower() == "windows" else "traceroute"
  param = "-h" if platform.system().lower() == "windows" else "-m"
  try:
    output = subprocess.check_output([command, param, str(max_hops), domain],
                     stderr=subprocess.STDOUT,
                     universal_newlines=True,
                     timeout=timeout)
    return output
  except subprocess.TimeoutExpired:
    return "Traceroute timed out"
  except subprocess.CalledProcessError:
    return "Traceroute failed"

def parse_traceroute(output):
  lines = output.split('\n')
  last_successful_ip = None
  ip_pattern = r'\d+\s+(?:\d+\s+ms|\*)\s+(?:\d+\s+ms|\*)\s+(?:\d+\s+ms|\*)\s+(\d+\.\d+\.\d+\.\d+)'
  for line in lines[4:]:  # Skip the first few lines as they're usually headers
    match = re.search(ip_pattern, line)
    if match:
      last_successful_ip = match.group(1)
    elif '*' in line and last_successful_ip:
      return last_successful_ip
  return None

def ip_lookup(ip):
  try:
    response = requests.get(f"http://ip-api.com/json/{ip}")
    data = response.json()
    return f"{data['country']}, {data['regionName']}, {data['city']}"
  except:
    return "IP lookup failed"

def process_domain(domain, timeout=30, max_hops=30):
  print(f"Processing {domain}...")
  traceroute_output = traceroute(domain, timeout, max_hops)

  if "timed out" in traceroute_output:
    return f"{domain}: Possible GFW detection (Traceroute timed out)"
  elif "failed" in traceroute_output:
    return f"{domain}: Traceroute failed"

  last_ip = parse_traceroute(traceroute_output)
  if not last_ip:
    return f"{domain}: No GFW detected (reached destination)"

  location = ip_lookup(last_ip)
  return f"{domain}: Possible GFW detected at {last_ip} ({location})"

def main(domains, timeout=30, max_hops=30, max_workers=8):
  results = []
  with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
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
  with open(f"ExperimentResult/{filename}", "w", newline="\n") as f:
    for result in results:
      f.write(f"{result}")

if __name__ == "__main__":
  main(domains, timeout=30, max_hops=30, max_workers=8)
