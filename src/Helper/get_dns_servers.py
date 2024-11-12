import csv

CSV_FILE = '../Import/dns_servers.csv'
def get_dns_servers() -> list:
    """
    @brief Retrieves a list of DNS servers from a CSV file.

    @return A list of DNS servers.

    @details This function reads a CSV file containing DNS server information and returns a list of DNS servers.
    """
    csv_file = CSV_FILE
    dns_servers = []
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            dns_servers.append(row[1])
    return dns_servers

def get_dns_servers_and_providers() -> dict:
    """
    @brief: Retrieves a dictionary of DNS servers and their providers.

    @return:
        dict: A dictionary containing DNS servers and their providers, grouped by region.
    """
    csv_file = CSV_FILE
    dns_servers = {}
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            region, dns_server, provider = row[0], row[1], row[2]
            if region not in dns_servers:
                dns_servers[region] = {}
            dns_servers[region][dns_server] = provider
    return dns_servers

