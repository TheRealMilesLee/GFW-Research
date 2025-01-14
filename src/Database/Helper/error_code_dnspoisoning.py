import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..DBOperations import ADC_db, BDC_db, Merged_db, MongoDBHandler

error_codes = ADC_db['ERROR_CODES']

def extract_timestamp_from_filename(filename):
    base_name = os.path.basename(filename)
    date_str = base_name.split('_')[1:4]
    return '-'.join(date_str)

def parse_error_line(line):
    domain_start = line.find('querying ') + len('querying ')
    domain_end = line.find(' with ')
    domain = line[domain_start:domain_end]

    dns_server_start = line.rfind('Server Do53:') + len('Server Do53:')
    dns_server_end = line.find('@53', dns_server_start)
    dns_server = line[dns_server_start:dns_server_end]

    record_type = 'A' if 'IN A' in line else 'AAAA' if 'IN AAAA' in line else 'N/A'

    error_code_start = line.rfind('answered ') + len('answered ')
    error_code_end = line.find(' ', error_code_start)
    error_code = line[error_code_start:error_code_end]

    if 'timed out' in line:
        error_code = 'timed out'

    return domain, dns_server, record_type, error_code

def process_file(filename, error_file_dir):
    error_file_path = os.path.join(error_file_dir, filename)
    timestamp = extract_timestamp_from_filename(filename)
    merged_data = defaultdict(lambda: {
        "timestamp": set(),
        "dns_server": set(),
        "error_code": set(),
        "error_reason": set(),
        "record_type": set()
    })

    with open(error_file_path, 'r') as file:
        data = file.read().splitlines()
        for line in data:
            domain, dns_server, record_type, error_code = parse_error_line(line)
            error_reason = line

            merged_data[domain]["timestamp"].add(timestamp)
            merged_data[domain]["dns_server"].add(dns_server)
            merged_data[domain]["error_code"].add(error_code)
            merged_data[domain]["error_reason"].add(error_reason)
            merged_data[domain]["record_type"].add(record_type)

    return merged_data

def merge_and_insert_error_codes():
    # Drop the db first before inserting
    error_codes.drop()
    if os.name == 'nt':
        error_file_dir = 'E:\\Developer\\SourceRepo\\GFW-Research\\Lib\\AfterDomainChange\\China-Mobile\\Error'
    else:
        error_file_dir = '/Users/silverhand/Developer/SourceRepo/GFW-Research/Lib/AfterDomainChange/China-Mobile/Error/'
    merged_data = defaultdict(lambda: {
        "timestamp": set(),
        "dns_server": set(),
        "error_code": set(),
        "error_reason": set(),
        "record_type": set()
    })

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_file, filename, error_file_dir) for filename in os.listdir(error_file_dir) if filename.endswith('.txt')]
        for future in as_completed(futures):
            file_data = future.result()
            for domain, data in file_data.items():
                merged_data[domain]["timestamp"].update(data["timestamp"])
                merged_data[domain]["dns_server"].update(data["dns_server"])
                merged_data[domain]["error_code"].update(data["error_code"])
                merged_data[domain]["error_reason"].update(data["error_reason"])
                merged_data[domain]["record_type"].update(data["record_type"])

    for domain, data in merged_data.items():
        error_codes.insert_one({
            "domain": domain,
            "timestamp": list(data["timestamp"]),
            "dns_server": list(data["dns_server"]),
            "error_code": list(data["error_code"]),
            "error_reason": list(data["error_reason"]),
            "record_type": list(data["record_type"])
        })

    # Create an index on the domain field
    error_codes.create_index("domain")

if __name__ == '__main__':
    merge_and_insert_error_codes()
