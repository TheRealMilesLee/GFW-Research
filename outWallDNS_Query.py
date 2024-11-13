import csv
import socket
import sys

from dns import resolver


# 检查是否有 IPv6 访问
def has_ipv6_access():
    try:
        # 尝试连接到一个 IPv6 地址（Google 的公共 IPv6 DNS 服务器）
        socket.create_connection(("2001:4860:4860::8888", 80), timeout=5)
        return True
    except (socket.timeout, OSError):
        return False

# 从 CSV 文件中读取域名和 DNS 服务器列表
def read_csv(file_path):
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        return [row[0] for row in reader]

# 通过指定的 DNS 服务器查询域名
def query_dns(domain, dns_server, query_type):
    try:
        res = resolver.Resolver()
        res.nameservers = [dns_server]
        answers = res.resolve(domain, query_type)
        return [answer.address for answer in answers]
    except (resolver.NoAnswer, resolver.NXDOMAIN, resolver.Timeout):
        return []

# 写入查询结果到 CSV 文件
def write_results_to_csv(results, output_file="./src/Import/CorrectIPResult.csv"):
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Domain", "DNS Server", "IP Address", "Type"])
        writer.writerows(results)
    print(f"结果已写入 {output_file}")

def main():
    # 检测 IPv6 访问
    if not has_ipv6_access():
        print("没有 IPv6 访问权限，程序退出。")
        sys.exit()

    # 读取域名列表和 DNS 服务器列表
    domain_list = read_csv("./src/Import/domains_list.csv")
    dns_servers = read_csv("./src/Import/dns_servers.csv")

    results = []

    for domain in domain_list:
        for dns_server in dns_servers:
            # 查询 A 记录 (IPv4)
            ipv4_addresses = query_dns(domain, dns_server, 'A')
            for ip in ipv4_addresses:
                results.append([domain, dns_server, ip, "IPv4"])

            # 查询 AAAA 记录 (IPv6)
            ipv6_addresses = query_dns(domain, dns_server, 'AAAA')
            for ip in ipv6_addresses:
                results.append([domain, dns_server, ip, "IPv6"])

    # 写入结果到 output.csv
    write_results_to_csv(results)

if __name__ == "__main__":
    main()
