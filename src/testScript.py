import re
import shutil
import subprocess


def traceroute(domain: str, use_ipv6: bool = False) -> list:
  """
  Executes a traceroute to the specified domain.

  @param domain The domain to trace the route to.
  @param use_ipv6 Whether to use IPv6 for the traceroute.
  @return A list of IP addresses in the route.
  @exception subprocess.CalledProcessError If the traceroute command fails.
  """
  if use_ipv6:
    if shutil.which('traceroute6'):
      command = ['traceroute6', domain]
    else:
      return []  # 如果traceroute6不可用，则返回空列表
  else:
    command = ['traceroute', domain]

  try:
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
      ips["ipv4"].extend(ipv4_ips["ipv4"])
    return ips
  except subprocess.CalledProcessError as e:
    print(f"Error running traceroute for {domain}: {e}")
    return []
  except subprocess.TimeoutExpired:
    print(f"Traceroute command timed out for {domain}")
    return []


if __name__ == '__main__':
  domain = 'google.com'
  results = traceroute(domain)
  print(f"Traceroute results for {domain}: {results}")
