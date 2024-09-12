import csv

def get_dns_servers() -> list:
    csv_file = 'dns_servers.csv'
    dns_servers = []
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            dns_servers.append(row[1])
    return dns_servers

def get_dns_servers_and_providers() -> dict:
    csv_file = 'dns_servers.csv'
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
