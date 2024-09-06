import dns.resolver
import dns.rdatatype

def query_dns(domain, dns_server):
  try:
    resolver = dns.resolver.Resolver()
    resolver.nameservers = [dns_server]

    ipv4_answers = []
    ipv6_answers = []

    try:
      ipv4_answers = resolver.resolve(domain, 'A')
    except dns.resolver.NoAnswer:
      print(f"No IPv4 records found for {domain}")

    try:
      ipv6_answers = resolver.resolve(domain, 'AAAA')
    except dns.resolver.NoAnswer:
      print(f"No IPv6 records found for {domain}")

    return {
      'domain': domain,
      'dns_server': dns_server,
      'ipv4': [answer.to_text() for answer in ipv4_answers],
      'ipv6': [answer.to_text() for answer in ipv6_answers]
    }
  except Exception as e:
    print(f"Error resolving {domain} on {dns_server}: {e}")
    return {
      'domain': domain,
      'dns_server': dns_server,
      'ipv4': [],
      'ipv6': []
    }

if __name__ == "__main__":
  # DNS servers to query
  dns_servers = {
    'china': [
      '114.114.114.114',  # 114DNS
      '114.114.115.115',  # 114 Alternative
      '223.5.5.5',        # AliDNS
      '223.6.6.6',        # AliDNS Alternative
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
  domain = "baidu.com"
  result = query_dns(domain, dns_servers['china'][0])
  print(result)
