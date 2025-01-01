import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
from DBOperations import CompareGroup_db, Merged_db, MongoDBHandler
import concurrent.futures
from tqdm import tqdm

# Merged_db constants
DNSPoisoning = MongoDBHandler(Merged_db["DNSPoisoning"])

categories = DNSPoisoning.distinct('error_code')

def DNSPoisoning_ErrorCode_Distribute():
  """
  Plot the distribution of error codes in the DNS poisoning data.
  """
  # Count the number of occurrences of each error code
  error_code_count = {}
  for category in categories:
    error_code_count[category] = DNSPoisoning.count_documents({'error_code': category})

  # Plot the distribution of error codes, with the number on each bar
  plt.figure(figsize=(12, 6))
  plt.bar(error_code_count.keys(), error_code_count.values())
  for category, count in error_code_count.items():
    plt.text(category, count, str(count), ha='center', va='bottom')
  plt.xlabel('Error Code')
  plt.ylabel('Number of Occurrences')
  plt.title('Distribution of Error Codes in DNS Poisoning Data')
  plt.savefig('DNSPoisoning_ErrorCode_Distribute.png')
  plt.close()

def process_domain(data):
  domain = data['domain']
  error_code_count = {}
  for category in categories:
    error_code_count[category] = DNSPoisoning.count_documents({'domain': domain, 'error_code': category})

  total_count = sum(error_code_count.values())
  folder_name = 'DNSPoisoning' if total_count > 0 else 'DNSPoisoning_Empty'

  plt.figure(figsize=(12, 6))
  plt.bar(error_code_count.keys(), error_code_count.values())
  for category, count in error_code_count.items():
    plt.text(category, count, str(count), ha='center', va='bottom')
  plt.xlabel('Error Code')
  plt.ylabel('Number of Occurrences')
  plt.title(f'Distribution of Error Codes in DNS Poisoning Data of {domain}')
  plt.savefig(f'{folder_name}/{domain}.png')
  plt.close()

if __name__ == '__main__':
  DNSPoisoning_ErrorCode_Distribute()
  dataFromMerged = DNSPoisoning.getAllDocuments()
  with concurrent.futures.ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(process_domain, dataFromMerged), total=len(dataFromMerged), desc='Processing domains'))
