import csv
import requests  # 使用requests库进行whois查询

def check_domain_existence(domain):
  try:
    response = requests.get(f"https://api.whois.com/v2/{domain}/json")
    response.raise_for_status()  # 检查响应状态码是否成功
    data = response.json()
    return data['response']['status'] == 'registered'
  except requests.exceptions.RequestException as e:
    print(f"Error checking {domain}: {e}")
    return False

def update_domains_list(domains_file):
  with open(domains_file, 'r') as f:
    lines = f.readlines()
  valid_domains = []
  for line in lines:
    domain = line.strip().split(',')[0]
    if check_domain_existence(domain):
      valid_domains.append(line)

  with open(domains_file, 'w') as f:
    f.writelines(valid_domains)


if __name__ == "__main__":
  update_domains_list('domains_list.csv')
