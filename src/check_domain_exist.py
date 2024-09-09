import csv
import whois

def check_domain_existence(domain):
  """
  检查域名是否存在。此处使用whois模块查询，需要安装该模块：pip install whois
  返回 True 存在，False 不存在。
  """
  try:
    whois_info = whois.whois(domain)
    return bool(whois_info.domain_name)
  except:
    return False

def main():
  with open('domains_list.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # 跳过第一行标题
    for row in reader:
      domain = row[0]
      if not check_domain_existence(domain):
        print(f"域名 {domain} 不存在，将其从列表中删除。")
        # 从文件中删除该域名（这里使用简单的修改文件内容）
        with open('domains_list.csv', 'r') as f:
          lines = f.readlines()
        with open('domains_list.csv', 'w') as f:
          for line in lines:
            if line.strip().split(',')[0] != domain:
              f.write(line)

if __name__ == "__main__":
  main()
