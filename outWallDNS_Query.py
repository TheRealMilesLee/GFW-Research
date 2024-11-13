import concurrent.futures
import csv

import dns.resolver as resolver

from src.scripts.get_dns_servers import get_dns_servers


# 从 CSV 文件中读取域名和 DNS 服务器列表
def read_csv(file_path):
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        return [row[0] for row in reader]

# 通过指定的 DNS 服务器查询域名
def query_dns(domain, dns_server, query_type):
    print(f"正在查询 {domain} 的 {query_type} 记录...")
    try:
        res = resolver.Resolver()
        res.nameservers = [dns_server]
        answers = res.resolve(domain, query_type)
        return [domain, dns_server, [answer.address for answer in answers], query_type]
    except (resolver.NoAnswer, resolver.NXDOMAIN, resolver.Timeout, resolver.NoNameservers):
        return [domain, dns_server, [], query_type]

# 写入查询结果到 CSV 文件
def write_results_to_csv(results, output_file="./src/Import/CorrectIPResult.csv"):
    print("正在写入结果到 CorrectIPResult.csv...")
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Domain", "DNS Server", "IP Address", "Type"])
        writer.writerows(results)
    print(f"结果已写入 {output_file}")

def main():
    # 读取域名列表和 DNS 服务器列表
    domain_list = read_csv("./src/Import/domains_list.csv")
    dns_servers = get_dns_servers()
    results = []
    batch_size = 8

    with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
        for i in range(0, len(domain_list), batch_size):
            batch = domain_list[i:i + batch_size]
            future_to_query = {
                executor.submit(query_dns, domain, dns_servers[0][0], 'A'): domain
                for domain in batch
            }

            for future in concurrent.futures.as_completed(future_to_query):
                domain = future_to_query[future]
                try:
                    result = future.result()
                    if result[2]:  # If there are IP addresses
                        for ip in result[2]:
                            results.append([result[0], result[1], ip, "IPv4"])
                except Exception as exc:
                    print(f'{domain} generated an exception: {exc}')

    # 写入结果到 output.csv
    write_results_to_csv(results)

if __name__ == "__main__":
    main()
