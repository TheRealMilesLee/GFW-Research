import asyncio
import csv
import os
from datetime import datetime, timedelta

import dns.asyncresolver
from get_dns_servers import get_dns_servers
import py7zr

# Timeout for connection attempts (in seconds)
TIMEOUT = 30
BATCH_SIZE = 128
CONCURRENT_TASKS = 128
WRITE_THRESHOLD = 2500


async def query_dns(domain: str, dns_server: str, record_type: str) -> dict:
  resolver = dns.asyncresolver.Resolver()
  resolver.nameservers = [dns_server]
  resolver.timeout = TIMEOUT
  resolver.lifetime = TIMEOUT + 5

  answers = []
  error_code = None
  error_reason = None

  try:
    print(f"Querying {domain} on {dns_server} for {record_type}")
    answers = await resolver.resolve(domain, record_type)
  except dns.resolver.Timeout:
    error_code = 'Timeout'
    error_reason = f"Timeout occurred for domain: {domain} on server: {dns_server}"
  except dns.resolver.NoAnswer:
    error_code = 'NoAnswer'
    error_reason = f"No answer for domain: {domain} on server: {dns_server}"
  except dns.resolver.NXDOMAIN:
    error_code = 'NXDOMAIN'
    error_reason = f"Non-existent domain: {domain} on server: {dns_server}"
  except dns.resolver.YXDOMAIN:
    error_code = 'YXDOMAIN'
    error_reason = f"Domain name should not exist: {domain} on server: {dns_server}"
  except dns.resolver.NoNameservers:
    error_code = 'NoNameservers'
    error_reason = f"No nameservers for domain: {domain} on server: {dns_server}"
  except dns.resolver.ServFail:
    error_code = 'ServFail'
    error_reason = f"Server failure for domain: {domain} on server: {dns_server}"
  except dns.resolver.Refused:
    error_code = 'REFUSED'
    error_reason = f"Server refused to answer for domain: {domain} on server: {dns_server}"
  except dns.resolver.FormErr:
    error_code = 'FORMERR'
    error_reason = f"Format error for domain: {domain} on server: {dns_server}"
  except Exception as e:
    error_code = 'UnknownError'
    error_reason = f"Unexpected error querying {domain} on {dns_server}: {e}"
  else:
    for rdata in answers:
      print(f"Record: {rdata}")

  return {
      'domain': domain,
      'dns_server': dns_server,
      'record_type': record_type,
      'answers': [answer.to_text() for answer in answers],
      'error_code': error_code,
      'error_reason': error_reason
  }


async def check_poisoning(domains: list, ipv4_dns_servers: list,
                          ipv6_dns_servers: list) -> list:
  results = []
  semaphore = asyncio.Semaphore(CONCURRENT_TASKS)

  async def sem_query_dns(domain, dns_server, record_type):
    async with semaphore:
      return await query_dns(domain, dns_server, record_type)

  tasks = [
      sem_query_dns(domain, dns_server, 'A') for domain in domains
      for dns_server in ipv4_dns_servers
  ] + [
      sem_query_dns(domain, dns_server, 'AAAA') for domain in domains
      for dns_server in ipv6_dns_servers
  ]

  for future in asyncio.as_completed(tasks):
    try:
      dns_results = await future
      results.append(dns_results)
    except Exception as e:
      error_folder_path = '/home/lhengyi/Developer/GFW-Research/Lib/CompareGroup/Error'
      os.makedirs(error_folder_path, exist_ok=True)
      error_filename = f"ErrorDomains_{datetime.now().strftime('%Y_%m_%d')}.txt"
      error_filepath = os.path.join(error_folder_path, error_filename)
      with open(error_filepath, "a") as error_file:
        error_file.write(f"Error querying domain with server: {e}\n")

  return results


def save_results(results: list, is_first_write=False) -> None:
  filename = f'DNS_Checking_Result_{datetime.now().strftime("%Y_%m_%d")}.csv'
  folder_path = '/home/lhengyi/Developer/GFW-Research/Lib/CompareGroup/DNSPoisoning'
  os.makedirs(folder_path, exist_ok=True)
  filepath = f"{folder_path}/{filename}"

  mode = "w" if is_first_write else "a"  # Use "a" mode to append data after the first write
  print(
      f"{'Creating' if is_first_write else 'Appending to'} results file at {filepath}"
  )

  try:
    with open(filepath, mode, newline="") as csvfile:
      fieldnames = [
          "timestamp", "domain", "dns_server", "record_type", "answers",
          "error_code", "error_reason"
      ]
      writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

      # Write header only on the first write
      if is_first_write:
        writer.writeheader()

      for row in results:
        writer.writerow({
            "timestamp": datetime.now().isoformat(),
            "domain": row['domain'],
            "dns_server": row['dns_server'],
            "record_type": row['record_type'],
            "answers": row['answers'],
            "error_code": row['error_code'],
            "error_reason": row['error_reason']
        })
  except Exception as e:
    print(f"Error saving results to file: {e}")


async def main():
  try:
    ipv4_dns_servers, ipv6_dns_servers = get_dns_servers()
    file_path = os.path.join(
        os.path.dirname(__file__),
        '/home/lhengyi/Developer/GFW-Research/src/Import/domains_list.csv'
    )

    with open(file_path, 'r') as file:
      reader = csv.reader(file)
      domains = [row[0].strip() for row in reader]

    print(f"Checking {len(domains)} domains for DNS poisoning")

    all_results = []  # Collect all results here
    for i in range(0, len(domains), BATCH_SIZE):
      batch = domains[i:i + BATCH_SIZE]
      results = await check_poisoning(batch, ipv4_dns_servers,
                                      ipv6_dns_servers)
      all_results.extend(results)  # Append batch results to all_results

      if len(all_results) >= WRITE_THRESHOLD:
        save_results(all_results, is_first_write=is_first_write)
        is_first_write = False  # After the first write, always append
        all_results.clear(
        )  # Clear results after saving to prepare for the next batch

    print(f"All batches completed at {datetime.now()}")

    # Save remaining results to CSV after all batches are processed
    if all_results:
      save_results(all_results, is_first_write=is_first_write)
      is_first_write = False  # After the first write, always append
      all_results.clear()  # Clear results after saving
      # After all domains are processed, compress the CSV file
      folder_path = '/home/lhengyi/Developer/GFW-Research/Lib/Data-CompareGroup/DNSPoisoning'
      filename = f'DNS_Checking_Result_{datetime.now().strftime("%Y_%m_%d")}.csv'
      filepath = f"{folder_path}/{filename}"
      compressed_filepath = f"{folder_path}/{filename}.7z"
      try:
        with py7zr.SevenZipFile(compressed_filepath,
                                'w',
                                filters=[{
                                    'id': py7zr.FILTER_LZMA2,
                                    'preset': 9
                                }]) as archive:
          archive.write(filepath, arcname=filename)
        print(f"CSV file compressed to {compressed_filepath}")
      except Exception as e:
        print(f"Error compressing CSV file: {e}")
      await asyncio.sleep(3600)  # Wait for 1 hour before the next check
  except Exception as e:
    print(f"Error in main execution: {e}")


if __name__ == "__main__":
  asyncio.run(main())
