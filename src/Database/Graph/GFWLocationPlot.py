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
from concurrent.futures import ThreadPoolExecutor
import matplotlib.lines as mlines

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


def plot_distribution(destination_db, output_folder, field, labels, title):
  """
  @brief Plots the distribution of documents based on a specified field.

  This function calculates the ratio of documents with the specified field in the
  MongoDB collection and plots a bar chart showing the total number of documents
  and the number with the field. The plot is saved as a PNG file in the
  specified output folder.

  @param destination_db The MongoDB collection object to query the documents from.
  @param output_folder The folder path where the output plot image will be saved.
  @param field The field to check for existence or a query dictionary.
  @param labels The labels for the bar chart.
  @param title The title of the plot.

  @return None
  """
  logger.info(f'Start to plot {title}')
  total_docs = destination_db.count_documents({})
  if isinstance(field, dict):
    query = field
  else:
    query = {field: {"$exists": True}}
  docs_with_field = destination_db.count_documents(query)
  ratio = docs_with_field / total_docs * 100

  fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)
  ax.bar(labels, [total_docs, docs_with_field])
  ax.text(labels[0], total_docs, total_docs, ha='center', va='bottom')
  ax.text(labels[1], docs_with_field, docs_with_field, ha='center', va='bottom')
  ax.set_xlabel('Document Type')
  ax.set_ylabel('Number of Documents')
  ax.set_title(f'{title} (Ratio: {ratio:.2f}%)')
  fig.savefig(f'{output_folder}/{title.replace(" ", "_")}.png')
  plt.close(fig)

def distribution_GFWL_rst_detected(destination_db, output_folder):
  """
  @brief Plots the distribution of documents with RST detected.

  @param destination_db The MongoDB collection object to query the documents from.
  @param output_folder The folder path where the output plot image will be saved.

  @return None
  """
  plot_distribution(
    destination_db,
    output_folder,
    "rst_detected",
    ['Total Docs', 'RST Detected'],
    'RST Detected Distribution'
  )

def distribution_GFWL_redirection_detected(destination_db, output_folder):
  """
  @brief Plots the distribution of documents with redirection detected.

  @param destination_db The MongoDB collection object to query for documents.
  @param output_folder The folder path where the output plot image will be saved.

  @return None
  """
  plot_distribution(
    destination_db,
    output_folder,
    {"redirection_detected": {"$elemMatch": {"$exists": True}}},
    ['Total Docs', 'Redirection Detected'],
    'Redirection Detected Distribution'
  )

def distribution_GFWL_Error(destination_db, output_folder):
  """
  @brief Plots the error distribution from the given database.

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
  plot_distribution(
    destination_db,
    output_folder,
    "invalid_ip",
    ['Valid IP', 'Invalid IP'],
    'IP Type Distribution'
  )

def ip_hops_core_path(destination_db, output_folder, domain=None, top_n=10, frequency_threshold=5):
  """
  @brief Constructs and visualizes core IP hops paths.

  This function constructs a graph based on IP hops, identifies top core paths by edge weight,
  assigns positions for layout, and visualizes the network graph, highlighting important edges.

  @param destination_db The MongoDB collection object to query the IP hops data from.
  @param output_folder The folder path where the IP path plot image will be saved.
  @param domain (Optional) The domain to filter IP hops.
  @param top_n The number of top core paths to identify based on edge weight.
  @param frequency_threshold The minimum frequency for an edge to be included in the graph.

  @return None
  """
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

  end_nodes = [node for node in subG.nodes if subG.out_degree(node) == 0]
  # Step 5: Visualize core hops with end nodes
  plt.figure(figsize=(15, 7))

  # Draw core nodes
  nx.draw_networkx_nodes(subG, pos, nodelist=[node for node in subG.nodes if node not in end_nodes],
                        node_size=200, node_color='blue', label='Core Nodes')

  # Draw end nodes
  nx.draw_networkx_nodes(subG, pos, nodelist=end_nodes, node_size=300, node_color='green', label='End Nodes')

  # Draw normal edges
  nx.draw_networkx_edges(subG, pos, edge_color='gray', arrowsize=10, label='Normal Edges')

  # Adjust label position
  label_pos = {node: (x, y - 5) for node, (x, y) in pos.items()}  # Label offset by 5 units
  nx.draw_networkx_labels(subG, label_pos, font_size=8, font_color='black')

  # Draw important edges
  top_edges_list = [(u, v) for u, v, _ in top_edges]
  nx.draw_networkx_edges(subG, pos, edgelist=top_edges_list, edge_color='red', width=2, label='Important Edges')

  # Create custom legend handles
  core_node_patch = mlines.Line2D([], [], marker='o', color='blue', linestyle='None',
                                  markersize=7, label='Core Nodes')
  end_node_patch = mlines.Line2D([], [], marker='o', color='green', linestyle='None',
                                 markersize=10, label='End Nodes')
  normal_edge_line = mlines.Line2D([], [], color='gray', linewidth=2, label='Normal Edges')
  important_edge_line = mlines.Line2D([], [], color='red', linewidth=2, label='Important Edges')

  # Add custom legend
  plt.legend(handles=[core_node_patch, end_node_patch, normal_edge_line, important_edge_line],
            scatterpoints=1, loc='best', fontsize=10)

  # Add labels for end nodes
  for node in end_nodes:
    x, y = pos[node]
    plt.text(x, y - 10, 'End', fontsize=8, color='green', ha='center')

  plt.title(f'Optimized Core IP Hops Network{" for " + domain if domain else ""}', fontsize=16)
  filename = f'Optimized_Core_IP_Hops_Network{"_" + domain if domain else ""}.png'
  plt.savefig(f'{output_folder}/IP_Path/{filename}', bbox_inches='tight')
  plt.close()

def ensure_folder_exists(folder):
  if not os.path.exists(folder):
    os.makedirs(folder)
if __name__ == '__main__':
  if os.name == 'nt':
    folders = [
      'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9',
      'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9\\IP_Path',
      'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11',
      'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11\\IP_Path',
      'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1',
      'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1\\IP_Path'
    ]
  elif os.name == 'posix':
    folders = [
      '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic2024-9',
      '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic2024-9/IP_Path',
      '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic2024-11',
      '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic2024-11/IP_Path',
      '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic2025-1',
      '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic2025-1/IP_Path'
    ]
  for folder in folders:
    ensure_folder_exists(folder)

  with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # 2024-9
    executor.submit(distribution_GFWL_rst_detected, GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
    executor.submit(distribution_GFWL_redirection_detected, GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
    executor.submit(distribution_GFWL_Error, GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
    executor.submit(distribution_GFWL_invalid_ip, GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')
    executor.submit(ip_hops_core_path, GFWLocation, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-9')

    # 2024-11
    executor.submit(distribution_GFWL_Error, merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
    executor.submit(distribution_GFWL_rst_detected, merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
    executor.submit(distribution_GFWL_redirection_detected, merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
    executor.submit(distribution_GFWL_invalid_ip, merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')
    executor.submit(ip_hops_core_path, merged_2024_Nov_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2024-11')

    # 2025-1
    executor.submit(distribution_GFWL_Error, merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
    executor.submit(distribution_GFWL_rst_detected, merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
    executor.submit(distribution_GFWL_redirection_detected, merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
    executor.submit(distribution_GFWL_invalid_ip, merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
    executor.submit(ip_hops_core_path, merged_2025_Jan_GFWL, 'E:\\Developer\\SourceRepo\\GFW-Research\\Pic\\2025-1')
  logger.info('All tasks completed.')
