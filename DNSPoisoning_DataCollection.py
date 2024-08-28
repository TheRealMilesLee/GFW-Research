import csv
import time
from datetime import datetime

import dns.resolver

# List of domains to check
domains = [
  "www.baidu.com",
  "www.bing.com",
  "www.github.com",
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


# DNS servers to query
dns_servers = {
  'china': [
    '114.114.114.114',  # 114DNS
    '114.114.115.115',  # 114 Alternative
    '223.5.5.5',        # AliDNS
    '119.29.29.29',     # DNSPod
    '180.76.76.76',     # Baidu
    '202.96.128.86',    # China Telecom
    '210.21.196.6',     # China Unicom
    '218.30.118.6',     # CUCC DNS
    '211.136.17.107',   # China Mobile
    '117.50.11.11',     # One DNS

  ],
  'global': [
    '8.8.8.8',          # Google
    '8.8.4.4',          # Google Alternative
    '1.1.1.1',          # Cloudflare
    '1.0.0.1',          # Cloudflare Alternative
    '9.9.9.9',          # Quad9
    '149.112.112.112',  # Quad9 Alternative
    '208.67.222.222',   # OpenDNS
    '208.67.220.220',   # OpenDNS Alternative
    '8.26.56.26',       # Comodo Secure DNS
    '8.20.247.20',      # Comodo Secure DNS Alternative
    '199.85.126.10',    # Norton ConnectSafe DNS
    '199.85.127.10',    # Norton ConnectSafe DNS Alternative
    '77.88.8.8',        # Yandex DNS
    '77.88.8.1',        # Yandex DNS Alternative
    '94.140.14.14',     # AdGuard DNS
    '94.140.15.15',     # AdGuard DNS Alternative
  ]
}

def query_dns(domain, dns_server):
  resolver = dns.resolver.Resolver()
  resolver.nameservers = [dns_server]
  results = {'ipv4': [], 'ipv6': []}
  try:
    # Query for IPv4 addresses
    answers = resolver.resolve(domain, 'A')
    results['ipv4'] = [answer.address for answer in answers]
  except Exception as e:
    results['ipv4'] = str(e)

  try:
    # Query for IPv6 addresses
    answers = resolver.resolve(domain, 'AAAA')
    results['ipv6'] = [answer.address for answer in answers]
  except Exception as e:
    results['ipv6'] = str(e)

  return results

def check_poisoning():
  print(f"Starting check at {datetime.now()} for domains: {domains}")
  results = []
  timestamp = datetime.now().isoformat()

  for domain in domains:
    china_results = {'ipv4': [], 'ipv6': []}
    for china_dns in dns_servers['china']:
      dns_results = query_dns(domain, china_dns)
      china_results['ipv4'].extend(dns_results['ipv4'])
      china_results['ipv6'].extend(dns_results['ipv6'])

    global_results = {'ipv4': [], 'ipv6': []}
    for global_dns in dns_servers['global']:
      dns_results = query_dns(domain, global_dns)
      global_results['ipv4'].extend(dns_results['ipv4'])
      global_results['ipv6'].extend(dns_results['ipv6'])

    is_poisoned_ipv4 = set(china_results['ipv4']) != set(global_results['ipv4'])
    is_poisoned_ipv6 = set(china_results['ipv6']) != set(global_results['ipv6'])
    is_poisoned = is_poisoned_ipv4 or is_poisoned_ipv6

    print(f"Domain: {domain} check completed")
    results.append({
      'timestamp': timestamp,
      'domain': domain,
      'china_result_ipv4': china_results['ipv4'],
      'china_result_ipv6': china_results['ipv6'],
      'global_result_ipv4': global_results['ipv4'],
      'global_result_ipv6': global_results['ipv6'],
      'is_poisoned': is_poisoned,
      'is_poisoned_ipv4': is_poisoned_ipv4,
      'is_poisoned_ipv6': is_poisoned_ipv6
    })

  return results

def save_results(results):
  filename = f'dns_poisoning_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
  with open(filename, "w", newline="") as csvfile:
    fieldnames = [
      "timestamp",
      "domain",
      "china_result_ipv4",
      "china_result_ipv6",
      "global_result_ipv4",
      "global_result_ipv6",
      "is_poisoned",
      "is_poisoned_ipv4",
      "is_poisoned_ipv6"
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
      writer.writerow(row)

def main():
  while True:
    results = check_poisoning()
    save_results(results)
    print(f"Check completed at {datetime.now()}")
    time.sleep(3600)  # Wait for 1 hour before next check


if __name__ == "__main__":
  main()
