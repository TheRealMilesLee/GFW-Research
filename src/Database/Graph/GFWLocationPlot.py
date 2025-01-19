import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
from ..DBOperations import Merged_db, MongoDBHandler
from collections import Counter
import os
import networkx as nx
import logging
import multiprocessing
from collections import Counter

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


def ip_hops_core_path(destination_db, output_folder, domain=None, top_n=10, frequency_threshold=5):
  G = nx.DiGraph()
  unique_edges = set()

  query = {"ips": {"$exists": True, "$ne": []}}
  if domain:
    query["domain"] = domain
  cursor = destination_db.find(query, {"ips": 1})

  total_paths = 0

  # Step 1: Parse data and construct graph
  edge_frequency = Counter()
  for doc in cursor:
    ips_strings = doc.get('ips', [])
    for ips_str in ips_strings:
      ips = [ip.strip() for ip in ips_str.split(',') if ip.strip()]
      if len(ips) < 2:
        continue
      hops = ips[1:]
      for i in range(len(hops) - 1):
        edge = (hops[i], hops[i + 1])
        edge_frequency[edge] += 1
        unique_edges.add(edge)
      total_paths += 1

  # Step 2: Add edges to graph based on frequency threshold
  for edge, freq in edge_frequency.items():
    if freq >= frequency_threshold:
      G.add_edge(*edge, weight=freq)

  # Step 3: Identify top N core paths by edge weight
  sorted_edges = sorted(G.edges(data=True), key=lambda x: x[2]['weight'], reverse=True)
  top_edges = sorted_edges[:top_n]
  core_nodes = set()
  for edge in top_edges:
    core_nodes.add(edge[0])
    core_nodes.add(edge[1])

  subG = G.subgraph(core_nodes).copy()

  # Step 4: Layered layout for hops
  sources = [node for node in subG.nodes if subG.in_degree(node) == 0]
  hop_levels = {source: 0 for source in sources}

  current_level = 1
  queue = list(sources)
  while queue:
    next_queue = []
    for node in queue:
      for neighbor in subG.successors(node):
        if neighbor not in hop_levels:
          hop_levels[neighbor] = current_level
          next_queue.append(neighbor)
    queue = next_queue
    current_level += 1

  # Assign positions
  levels = {}
  for node, level in hop_levels.items():
    levels.setdefault(level, []).append(node)
  pos = {}
  x_gap = 10
  y_gap = 20
  for level, nodes in levels.items():
    for i, node in enumerate(nodes):
      pos[node] = (i * x_gap, -level * y_gap)

  # 找到终结节点（出度为 0 的节点）
  end_nodes = [node for node in subG.nodes if subG.out_degree(node) == 0]
  # Step 5: Visualize core hops with end nodes
  plt.figure(figsize=(15, 7))

  # 绘制普通节点
  nx.draw_networkx_nodes(subG, pos, nodelist=[node for node in subG.nodes if node not in end_nodes],
                        node_size=200, node_color='blue')

  # 绘制终结节点
  nx.draw_networkx_nodes(subG, pos, nodelist=end_nodes, node_size=300, node_color='green', label='End Nodes')

  # 绘制边
  nx.draw_networkx_edges(subG, pos, edge_color='gray', arrowsize=10)

  # 调整标签的位置
  label_pos = {node: (x, y - 5) for node, (x, y) in pos.items()}  # 标签下移 5 单位
  nx.draw_networkx_labels(subG, label_pos, font_size=8, font_color='black')

  # 高亮最重要的边
  top_edges = [(u, v) for u, v, _ in top_edges]
  nx.draw_networkx_edges(subG, pos, edgelist=top_edges, edge_color='red', width=2)
  # 添加图例说明红线是最重要的边
  plt.legend(['Normal Edge', 'Important Edge'], fontsize=10)


  # 标注终结节点为 "End"
  for node in end_nodes:
    x, y = pos[node]
    plt.text(x, y - 10, 'End', fontsize=8, color='green', ha='center')

  # 添加图例
  plt.legend(scatterpoints=1, loc='best', fontsize=10)

  plt.title(f'Optimized Core IP Hops Network{" for " + domain if domain else ""}', fontsize=16)
  filename = f'Optimized_Core_IP_Hops_Network{"_" + domain if domain else ""}.png'
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
    'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1',
    'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1\\IP_Path'
  ]
  for folder in folders:
    ensure_folder_exists(folder)

  distribution_GFWL_rst_detected(GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9',)
  distribution_GFWL_redirection_detected(GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_GFWL_Error(GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  distribution_GFWL_invalid_ip(GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
  ip_hops_core_path(GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')

  distribution_GFWL_Error(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_GFWL_rst_detected(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_GFWL_redirection_detected(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  distribution_GFWL_invalid_ip(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
  ip_hops_core_path(merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')

  distribution_GFWL_Error(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_GFWL_rst_detected(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_GFWL_redirection_detected(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  distribution_GFWL_invalid_ip(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  ip_hops_core_path(merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
