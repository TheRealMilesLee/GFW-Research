import concurrent.futures
import csv
import socket
import time
import os
import platform
from datetime import datetime

import dns.resolver

# List of domains to check
domains = [
  "www.baidu.com",
  "www.bing.com",
  "github.com",
  "www.docker.io",
  "www.ucdavis.edu",
  "www.truman.edu",
  "ipv6.google.com",
  "ipv6.test-ipv6.com",
  "www.kame.net",
  "ipv6.he.net",
  "ipv6.eurov6.org",
  "www.sixxs.net",
  "ipv6.ripe.net",
  "www.isc.org",
  "ipv6.facebook.com",
  "ipv6.bbc.co.uk",
  "ipv6.microsite.prod.nic.ad.jp",
  "www.v6.facebook.com",
  "ipv6.youtube.com",
  "ipv6.twitter.com",
  "ipv6.reddit.com",
  "ipv6.xnxx.com",
  "ipv6.netflix.com",
  "ipv6.apnic.net",
  "ipv6.baidu.com",
  "ipv6.sina.com.cn",
  "ipv6.taobao.com",
  "ipv6.juniper.net",
  "ipv6.cisco.com",
  "ipv6.yandex.com",
  "ipv6.wipo.int",
  "ipv6.cloudflare.com",
  "ipv6.amazon.com",
  "ipv6.apple.com",
  "ipv6.ibm.com",
  "ipv6.microsoft.com",
  "www.reddit.com",
  "www.twitter.com",
  "www.facebook.com",
  "www.instagram.com",
  "www.google.com",
  "www.youtube.com",
  "www.dropbox.com",
  "www.wikipedia.org",
  "www.linkedin.com",
  "www.whatsapp.com",
  "www.telegram.org",
  "www.medium.com",
  "www.blogspot.com",
  "www.wordpress.com",
  "www.vimeo.com",
  "www.tumblr.com",
  "www.slack.com",
  "www.github.com",
  "www.netflix.com",
  "www.flickr.com",
  "www.soundcloud.com",
  "www.quora.com",
  "www.pinterest.com",
  "www.snapchat.com",
  "www.twitch.tv",
  "www.periscope.tv",
  "www.disqus.com",
  "www.dailymotion.com",
  "www.meetup.com",
  "www.tiktok.com",
  "www.facebook.com",
  "www.twitter.com",
  "www.instagram.com",
  "www.linkedin.com",
  "www.tiktok.com",
  "www.snapchat.com",
  "www.reddit.com",
  "www.pinterest.com",
  "www.tumblr.com",
  "www.quora.com",
  "www.medium.com",
  "www.whatsapp.com",
  "www.telegram.org",
  "www.wechat.com",
  "www.youtube.com",
  "www.vimeo.com",
  "www.dailymotion.com",
  "www.twitch.tv",
  "www.discord.com",
  "www.vk.com",
  "www.line.me",
  "www.weibo.com",
  "www.douyin.com",
  "www.kuaishou.com",
  "www.qq.com",
  "www.signal.org",
  "www.clubhouse.com",
  "www.meetup.com",
  "www.viber.com",
  "www.periscope.tv",
]


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
  print(f"Starting IP blocking check at {datetime.now()}")
  results = []
  timestamp = datetime.now().isoformat()

  # First, get IPs for all domains
  ip_dict = get_ips_from_domains(domains)

  # Then cleanup the IP dictionary, deleted the domains that did not resolve or have no IPs
  ip_dict = {domain: ips for domain, ips in ip_dict.items() if ips['ipv4'] or ips['ipv6']}
  # Output to the file for the domains that has problem IP or have problem on resolving IP
  with open('problem_domains.txt', 'w') as f:
    for domain in domains:
      if domain not in ip_dict:
        f.write(f"{domain} did not resolve\n")
      elif not ip_dict[domain]['ipv4'] and not ip_dict[domain]['ipv6']:
        f.write(f"{domain} has no IPs\n")

  for domain, ips in ip_dict.items():
    print(f"Checking domain {domain}, IPs: {ips}")
    for ip_type in ['ipv4', 'ipv6']:
      for ip in ips[ip_type]:
        ports_to_check = ['80', '443']
        print(f"Checking {ip} for domain {domain} with ports {ports_to_check}")
        for port in ports_to_check:
          is_accessible = check_ip(ip, int(port))
          results.append({
            'timestamp': timestamp,
            'domain': domain,
            'ip': ip,
            'ip_type': ip_type,
            'port': port,
            'is_accessible': is_accessible
          })

  print(f"IP blocking check completed at {datetime.now()}")
  return results

def save_results(results):
  filename = f'IP_blocking_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
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
  with open(filepath, 'w', newline='') as csvfile:
    fieldnames = ['timestamp', 'domain', 'ip', 'ip_type', 'port', 'is_accessible']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
      writer.writerow(row)

def main():
  while True:
    results = run_checks()
    save_results(results)
    print(f"IP blocking check completed at {datetime.now()}")
    time.sleep(3600)  # Wait for 1 hour before next check

if __name__ == "__main__":
  main()
