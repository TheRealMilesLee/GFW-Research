import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
from ..DBOperations import Merged_db, MongoDBHandler
from collections import Counter
import os
import networkx as nx
import logging
import multiprocessing

CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 256)  # Dynamically set workers

# Config Logger
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Merged_db constants
GFWLocation = MongoDBHandler(Merged_db["TraceRouteResult"])
merged_2024_Nov_GFWL = MongoDBHandler(Merged_db["2024_Nov_GFWL"])
merged_2025_Jan_GFWL = MongoDBHandler(Merged_db["2025_GFWL"])


def distribution_GFWL_rst_detected(destination_db, output_folder):
  """
  @brief Plots the distribution of documents with RST detected in the given database.

  This function calculates the ratio of documents with the "rst_detected" field in the
  specified MongoDB collection and plots a bar chart showing the total number of documents
  and the number of documents with RST detected. The plot is saved as a PNG file in the
  specified output folder.

  @param destination_db The MongoDB collection object to query the documents from.
  @param output_folder The folder path where the output plot image will be saved.

  @return None
  """
  logger.info('Start to plot RST Detected Distribution')
  total_docs = destination_db.count_documents({})
  docs_with_rst_detected = destination_db.count_documents({"rst_detected": { "$exists": True }})
  rst_detected_ratio = docs_with_rst_detected / total_docs * 100

  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)
  ax.bar(['Total Docs', 'RST Detected'], [total_docs, docs_with_rst_detected])
  ax.text('Total Docs', total_docs, total_docs, ha='center', va='bottom')
  ax.text('RST Detected', docs_with_rst_detected, docs_with_rst_detected, ha='center', va='bottom')
  ax.set_xlabel('Document Type')
  ax.set_ylabel('Number of Documents')
  ax.set_title(f'RST Detected Distribution (Ratio: {rst_detected_ratio:.2f}%)')
  fig.savefig(f'{output_folder}/RST_Detected_Distribution.png')
  plt.close(fig)

def distribution_GFWL_redirection_detected(destination_db, output_folder):
  """
  @brief Plots the distribution of documents with redirection detected in the given database.

  This function counts the total number of documents and the number of documents with redirection detected
  in the specified MongoDB collection. It then plots a bar chart showing the distribution and saves the
  plot as a PNG file in the specified output folder.

  @param destination_db The MongoDB collection object to query for documents.
  @param output_folder The folder path where the output plot image will be saved.

  @return None
  """
  logger.info('Start to plot Redirection Detected Distribution')
  total_docs = destination_db.count_documents({})
  docs_with_redirection_detected = destination_db.count_documents({"redirection_detected": { "$elemMatch": { "$exists": True } }})
  redirection_detected_ratio = docs_with_redirection_detected / total_docs * 100

  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)
  ax.bar(['Total Docs', 'Redirection Detected'], [total_docs, docs_with_redirection_detected])
  ax.text('Total Docs', total_docs, total_docs, ha='center', va='bottom')
  ax.text('Redirection Detected', docs_with_redirection_detected, docs_with_redirection_detected, ha='center', va='bottom')
  ax.set_xlabel('Document Type')
  ax.set_ylabel('Number of Documents')
  ax.set_title(f'Redirection Detected Distribution (Ratio: {redirection_detected_ratio:.2f}%)')
  fig.savefig(f'{output_folder}/Redirection_Detected_Distribution.png')
  plt.close(fig)

def distribution_GFWL_Error(destination_db, output_folder):
  """
  @brief Plots the error distribution from the given database and saves the plot as an image.

  This function retrieves documents from the specified database, calculates the error ratio,
  counts the occurrences of each error, and generates a bar plot showing the distribution
  of errors. The plot is saved as an image in the specified output folder.

  @param destination_db The database connection object to retrieve documents from.
  @param output_folder The folder path where the error distribution plot image will be saved.

  @return None
  """
  logger.info('Start to plot Error Distribution')
  total_docs = destination_db.count_documents({})
  docs_with_error = destination_db.count_documents({"error": { "$elemMatch": { "$exists": True } }})
  error_ratio = docs_with_error / total_docs * 100

  error_counts = Counter()
  docs = destination_db.find({"error": { "$elemMatch": { "$exists": True } }})
  for doc in docs:
    for error in doc['error']:
      error_counts[error] += 1

  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)
  ax.bar(error_counts.keys(), error_counts.values())
  for error, count in error_counts.items():
    ax.text(error, count, str(count), ha='center', va='bottom')
  ax.set_xlabel('Error')
  ax.set_ylabel('Number of Occurrences')
  ax.set_title(f'Error Distribution (Ratio: {error_ratio:.2f}%)')
  fig.savefig(f'{output_folder}/Error_Distribution.png', bbox_inches='tight')
  plt.close(fig)


def distribution_GFWL_invalid_ip(destination_db, output_folder):
  """
  @brief Plots the distribution of valid and invalid IP addresses in the database.

  This function calculates the ratio of documents with invalid IP addresses to the total number of documents
  in the specified database. It then generates a bar plot showing the number of valid and invalid IP addresses
  and saves the plot to the specified output folder.

  @param destination_db The database connection object to query for documents.
  @param output_folder The folder path where the plot image will be saved.

  @return None
  """
  logger.info('Start to plot Invalid IP Distribution')
  total_docs = destination_db.count_documents({})
  docs_with_invalid_ip = destination_db.count_documents({"invalid_ip": { "$exists": True }})
  invalid_ip_ratio = docs_with_invalid_ip / total_docs * 100

  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)
  ax.bar(['Valid IP', 'Invalid IP'], [total_docs - docs_with_invalid_ip, docs_with_invalid_ip])
  ax.text('Valid IP', total_docs - docs_with_invalid_ip, total_docs - docs_with_invalid_ip, ha='center', va='bottom')
  ax.text('Invalid IP', docs_with_invalid_ip, docs_with_invalid_ip, ha='center', va='bottom')
  ax.set_xlabel('IP Type')
  ax.set_ylabel('Number of Occurrences')
  ax.set_title(f'IP Type Distribution (Ratio: {invalid_ip_ratio:.2f}%)')
  fig.savefig(f'{output_folder}/IP_Type_Distribution.png')
  plt.close(fig)


def ip_hops_path(destination_db, output_folder, domain=None):
  G = nx.DiGraph()
  unique_edges = set()

  query = {"ips": {"$exists": True, "$ne": []}}
  if domain:
    query["domain"] = domain
  cursor = destination_db.find(
    query,
    {"ips": 1}
  )

  batch_size = 10000
  batch_edges = []
  batch_nodes = set()
  total_paths = 0
  total_edges = 0

  for doc in cursor:
    ips_strings = doc.get('ips', [])
    for ips_str in ips_strings:
      ips = [ip.strip() for ip in ips_str.split(',') if ip.strip()]
      if len(ips) < 2:
        continue
      hops = ips[1:]
      for i in range(len(hops) - 1):
        edge = (hops[i], hops[i+1])
        if edge not in unique_edges:
          batch_edges.append(edge)
          unique_edges.add(edge)
          total_edges += 1
      batch_nodes.update(hops)
      total_paths += 1

    if len(batch_edges) >= batch_size:
      G.add_edges_from(batch_edges)
      batch_edges = []
    if len(batch_nodes) >= batch_size:
      G.add_nodes_from(batch_nodes)
      batch_nodes = set()

  if batch_edges:
    G.add_edges_from(batch_edges)
  if batch_nodes:
    G.add_nodes_from(batch_nodes)

  logger.info(f'Total paths processed: {total_paths}')
  logger.info(f'Total edges added to the graph: {total_edges}')
  logger.info(f'Total unique nodes: {len(G.nodes)}')

  sources = [node for node in G.nodes if G.in_degree(node) == 0]
  if not sources:
    sources = list(G.nodes)[:1]
  hop_levels = {}
  current_level = 0
  queue = []
  for source in sources:
    hop_levels[source] = current_level
    queue.append(source)

  while queue:
    next_queue = []
    current_level += 1
    for node in queue:
      for neighbor in G.successors(node):
        if neighbor not in hop_levels:
          hop_levels[neighbor] = current_level
          next_queue.append(neighbor)
    queue = next_queue

  levels = {}
  for node, level in hop_levels.items():
    levels.setdefault(level, []).append(node)

  pos = {}
  x_gap = 0.5
  y_gap = 0.5
  for level in sorted(levels.keys()):
    nodes = levels[level]
    for i, node in enumerate(nodes):
      pos[node] = (i * x_gap, level * y_gap)

  plt.figure(figsize=(100, 50))  # 根据需要调整图形尺寸
  nx.draw_networkx_nodes(G, pos, node_size=20, node_color='blue', alpha=0.6)
  nx.draw_networkx_edges(G, pos, arrowstyle='->', arrowsize=5, edge_color='gray', alpha=0.3)
  nx.draw_networkx_labels(G, pos, font_size=6, font_color='black')
  plt.title(f'IP Hops Path Network{" for " + domain if domain else ""}')
  filename = f'IP_Hops_Path_Network{"_" + domain if domain else ""}.png'
  plt.savefig(f'{output_folder}/IP_Path/{filename}', bbox_inches='tight')
  plt.close()

def ensure_folder_exists(folder):
  if not os.path.exists(folder):
    os.makedirs(folder)
if __name__ == '__main__':
  folders = [
    'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9',
    'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9\\IP_Path',
    'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11',
    'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11\\IP_Path',
    'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1'
    'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1\\IP_Path'
  ]
  for folder in folders:
    ensure_folder_exists(folder)

  distribution_GFWL_rst_detected(GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9',)
  distribution_GFWL_redirection_detected(GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_GFWL_Error(GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_GFWL_invalid_ip(GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  ip_hops_path(GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')

  distribution_GFWL_Error(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_GFWL_rst_detected(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_GFWL_redirection_detected(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_GFWL_invalid_ip(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  ip_hops_path(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')

  distribution_GFWL_Error(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_GFWL_rst_detected(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_GFWL_redirection_detected(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_GFWL_invalid_ip(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  ip_hops_path(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
