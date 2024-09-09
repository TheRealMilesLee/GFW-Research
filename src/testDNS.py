import socket

dns_list = [
    '114.114.114.114',  # 114DNS
    '114.114.115.115',  # 114 Alternative
    '223.5.5.5',        # AliDNS
    '223.6.6.6',        # AliDNS Alternative
    '119.29.29.29',     # DNSPod
    '180.76.76.76',     # Baidu
    '202.96.128.86',    # China Telecom
    '210.21.196.6',     # China Unicm
    '218.30.118.6',     # CUCC DNS
    '211.136.17.107',   # China Mobile
    '117.50.11.11',      # One DNS
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

for dns in dns_list:
    try:
        response = socket.gethostbyname("15min.lt")
        if response != '':
            print(f"DNS {dns} 是可用的")
        else:
            print(f"DNS {dns} 不是可用的")
    except socket.gaierror as e:
        print(f"发生错误，无法检查 DNS {dns}")
