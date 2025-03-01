import csv

DNS_SERVERS = '/home/lhengyi/Developer/GFW-Research/src/Import/dns_servers.csv'


def get_dns_servers() -> tuple:
  """
    @brief Retrieves a list of DNS servers from a CSV file.

    @return A tuple of lists containing IPV4 and IPV6 DNS servers.

    @details This function reads a CSV file containing IPV4 and IPV6 DNS server information and returns a tuple of lists of DNS servers.
    """
  csv_file = DNS_SERVERS
  ipv4_dns_servers = []
  ipv6_dns_servers = []
  with open(csv_file, 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row
    for row in reader:
      ipv4_dns_servers.append(row[1])
      # check row[2] for ipv6 DNS servers, if is not empty, append to ipv6_dns_servers
      if row[2]:
        ipv6_dns_servers.append(row[2])
  return ipv4_dns_servers, ipv6_dns_servers


if __name__ == '__main__':
  print(get_dns_servers())
