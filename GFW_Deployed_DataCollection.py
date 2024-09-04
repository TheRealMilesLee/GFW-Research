import subprocess, re, requests, concurrent.futures, platform, time, os
from datetime import datetime

def traceroute(domain, timeout=30, max_hops=30):
  system = platform.system().lower()
  if system == "windows":
    command = ["tracert", "-h", str(max_hops), domain]
  elif system == "linux":
    command = ["tracepath", "-m", str(max_hops), domain]
  elif system == "darwin":  # macOS
    command = ["traceroute", "-m", str(max_hops), domain]
  else:
    raise Exception("Unsupported operating system")

  try:
    output = subprocess.check_output(command,
             stderr=subprocess.STDOUT,
             timeout=timeout)
    return output.encode('utf-8').decode('utf-8', errors='ignore')  # Encode the output as bytes and then decode it as UTF-8 with ignoring errors
  except subprocess.TimeoutExpired as e:
    # Check if "timed out" occurs 30 times consecutively
    if "timed out" * 30 in e.output:
      raise Exception("Traceroute timed out")
    else:
      return "Traceroute timed out"
  except subprocess.CalledProcessError:
    return "Traceroute failed"

def parse_traceroute(output):
  lines = output.split('\n')
  last_successful_ip = None

  system = platform.system().lower()
  if system == "windows":
    ip_pattern = r'\d+\s+(?:\d+\s+ms|\*)\s+(?:\d+\s+ms|\*)\s+(?:\d+\s+ms|\*)\s+(\d+\.\d+\.\d+\.\d+)'
  else:  # Linux or macOS
    ip_pattern = r'\s*\d+:\s+(\d+\.\d+\.\d+\.\d+)'

  for line in lines[4:]:  # Skip the first few lines as they're usually headers
    match = re.search(ip_pattern, line)
    if match:
      last_successful_ip = match.group(1)
    elif ('*' in line or 'no reply' in line.lower()) and last_successful_ip:
      return last_successful_ip
  return None

def ip_lookup(ip):
  max_retries = 5
  retries = 0
  while retries < max_retries:
    try:
      response = requests.get(f"http://ip-api.com/json/{ip}")
      data = response.json()
      return f"{data['country']}, {data['regionName']}, {data['city']}"
    except:
      retries += 1
  return "IP lookup failed"

def process_domain(domain, timeout=120, max_hops=60):
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

def main(domains, timeout=30, max_hops=30):
  results = []
  with concurrent.futures.ThreadPoolExecutor() as executor:
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
  folder_path = ""
  system = platform.system().lower()
  if system == "linux":
    folder_path = 'ExperimentResult/CompareGroup/GFWLocation/'
    os.makedirs(folder_path, exist_ok=True)
  elif system == "darwin":  # macOS
    folder_path = f'ExperimentResult/Mac/GFWDeployed/'
    os.makedirs(folder_path, exist_ok=True)
  else:
    folder_path = f"ExperimentResult/GFWLocation/"
    os.makedirs(folder_path, exist_ok=True)
  filepath = f"{folder_path}/{filename}"
  with open(filepath, "w") as f:
    for result in results:
      f.write(f"{result}\n")

if __name__ == "__main__":
    # read domains from file
  file_path = os.path.join(os.path.dirname(__file__), 'domains_list.csv')
  with open(file_path, 'r') as file:
      domains = [line.strip() for line in file]
  while True:
    main(domains, timeout=120, max_hops=60)
    print(f"Check completed at {datetime.now()}")
    time.sleep(1440)  # Wait for 4 hours before next check
