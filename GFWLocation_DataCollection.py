import concurrent.futures, os, platform, re, subprocess, time, socket, requests, geoip2.database, os
from urllib.request import urlretrieve
from datetime import datetime
# Constants
GEOIP_DB_URL = "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb"
GEOIP_DB_PATH = os.path.join(os.path.dirname(__file__), "GeoLite2-City.mmdb")

def download_geoip_database():
  """Download the GeoIP database if it doesn't exist."""
  if not os.path.exists(GEOIP_DB_PATH):
    print("Downloading GeoIP database...")
    urlretrieve(GEOIP_DB_URL, GEOIP_DB_PATH)
    print("GeoIP database downloaded successfully.")


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
  ip_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
  ips = []
  for line in lines:
    found_ips = re.findall(ip_pattern, line)
    if found_ips:
      ips.extend(found_ips)
  return ips

def ip_lookup_with_fallback(ip):
  """Perform IP geolocation with fallback to local GeoIP database."""
  # Try online lookup first
  online_result = ip_lookup_online(ip)
  if online_result != "IP lookup failed":
    return online_result
  # Fallback to local database
  return ip_lookup_local(ip)

def ip_lookup_online(ip):
  """Perform online IP lookup (existing implementation)."""
  retries = 0
  while retries < 3:
    try:
      response = requests.get(f"http://ip-api.com/json/{ip}", timeout=5)
      data = response.json()
      if data['status'] == 'success':
        return f"{data.get('country', 'Unknown')}, {data.get('regionName', 'Unknown')}, {data.get('city', 'Unknown')}"
      else:
        return "IP lookup failed"
    except requests.RequestException:
      retries += 1
      time.sleep(1)
  return "IP lookup failed"

def ip_lookup_local(ip):
  """Perform IP lookup using local GeoIP database."""
  try:
    with geoip2.database.Reader(GEOIP_DB_PATH) as reader:
      response = reader.city(ip)
      country = response.country.name
      region = response.subdivisions.most_specific.name if response.subdivisions else "Unknown"
      city = response.city.name if response.city.name else "Unknown"
      return f"{country}, {region}, {city}"
  except geoip2.errors.AddressNotFoundError:
    return "IP not found in local database"
  except Exception as e:
    return f"Local IP lookup failed: {str(e)}"

# Modified process_domain function
def process_domain(domain, timeout=60, max_hops=30):
  print(f"Processing {domain}...")
  traceroute_output = traceroute(domain, timeout, max_hops)
  ips = parse_traceroute(traceroute_output)
  if not ips:
    return f"{domain}: No IP addresses found in traceroute"
  try:
    destination_ip = socket.gethostbyname(domain)
  except socket.gaierror:
    return f"{domain}: Unable to resolve domain"
  if destination_ip in ips:
    return f"{domain}: No GFW detected (Reached destination {destination_ip})"
  last_ip = ips[-1]
  location = ip_lookup_with_fallback(last_ip)
  return f"{domain}: Possible GFW detection at {last_ip} ({location})"
# Main function and other parts of the script remain the same

def main(domains, timeout=120, max_hops=60):
  results = []
  with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
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
