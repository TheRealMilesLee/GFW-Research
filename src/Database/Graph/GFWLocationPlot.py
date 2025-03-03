import matplotlib

matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
from ..DBOperations import Merged_db, MongoDBHandler, ADC_db
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
adc_db_2024_Nov_GFWL = MongoDBHandler(
    ADC_db["ChinaMobile-GFWLocation-2024-November"])
adc_db_2025_Jan_GFWL = MongoDBHandler(
    ADC_db["ChinaMobile-GFWLocation-2025-January"])


def ip_hops_core_path(destination_db,
                      output_folder,
                      domain=None,
                      top_n=50,
                      frequency_threshold=25):
  """
    Constructs and visualizes core IP hops paths.
    """
  G = nx.DiGraph()
  edge_frequency = Counter()
  ip_frequency = Counter()
  hops_list = []

  query = {"ips": {"$exists": True, "$ne": []}}
  if domain:
    query["domain"] = domain
  cursor = destination_db.find(query, {"ips": 1})

  for doc in cursor:
    ips_strings = doc.get('ips', [])
    for ips_str in ips_strings:
      ips = [ip.strip() for ip in ips_str.split(';') if ip.strip()]
      if len(ips) <= 2:
        continue
      if ips[0] == '127.0.0.1':
        ips[0] = '192.168.0.1'  # 统一改为起点
      hops = ips[1:]
      hops_list.append(hops)
      for ip in hops:
        ip_frequency[ip] += 1

  # 过滤低频路径 & 计算边频率, 去掉192.168.0.1的回环
  filtered_hops_list = []
  end_nodes = set()
  for hops in hops_list:
    if any(ip_frequency[ip] >= frequency_threshold for ip in hops):
      hops.insert(0, '192.168.0.1')
      filtered_hops_list.append(hops)
      end_nodes.add(hops[-1])
      for i in range(len(hops) - 1):
        edge_frequency[(hops[i], hops[i + 1])] += 1

  # 只添加满足频率要求的边
  for edge, freq in edge_frequency.items():
    if freq >= frequency_threshold:
      G.add_edge(*edge, weight=freq)

  # 获取 Top N 核心路径
  top_edges = sorted(G.edges(data=True),
                     key=lambda x: x[2]['weight'],
                     reverse=True)[:top_n]
  core_nodes = {node for edge in top_edges for node in edge[:2]}
  subG = G.subgraph(core_nodes).copy()
  end_nodes &= set(subG.nodes)

  # 计算层次布局
  hop_levels = {'192.168.0.1': 0}
  queue, current_level = ['192.168.0.1'], 1
  while queue:
    next_queue = []
    for node in queue:
      for neighbor in subG.successors(node):
        if neighbor not in hop_levels:
          hop_levels[neighbor] = current_level
          next_queue.append(neighbor)
    queue, current_level = next_queue, current_level + 1

  # 生成布局
  levels = {}
  for node, level in hop_levels.items():
    levels.setdefault(level, []).append(node)

  pos = {
      node: (i * 20 - len(nodes) * 10, -level * 40)
      for level, nodes in levels.items()
      for i, node in enumerate(nodes)
  }

  # 确保所有节点有位置
  pos.update({node: (0, 0) for node in subG.nodes if node not in pos})
  # 如果(0, 0)或(0, 1)或(1, 0)已被占用，向右移动
  occupied_positions = {(0, 0), (0, 1), (1, 0)}
  if any(pos.get(node) in occupied_positions for node in pos):
    pos = {node: (x + 15, y) for node, (x, y) in pos.items()}

  # 画图
  plt.figure(figsize=(20, 7))

  # 核心节点（蓝色）
  nx.draw_networkx_nodes(subG,
                         pos,
                         nodelist=set(subG.nodes) - end_nodes,
                         node_size=200,
                         node_color='blue',
                         label='Core Nodes')

  # 终点节点（绿色）
  nx.draw_networkx_nodes(subG,
                         pos,
                         nodelist=end_nodes,
                         node_size=300,
                         node_color='green',
                         label='End Nodes')

  # 普通边（灰色）
  nx.draw_networkx_edges(subG,
                         pos,
                         edge_color='gray',
                         arrowsize=10,
                         label='Normal Edges')

  # 重要边（红色）
  nx.draw_networkx_edges(subG,
                         pos,
                         edgelist=[(u, v) for u, v, _ in top_edges],
                         edge_color='red',
                         width=2,
                         label='Frequent Edges')

  # 调整标签
  nx.draw_networkx_labels(subG, {
      node: (x, y + 10)
      for node, (x, y) in pos.items()
  },
                          font_size=8,
                          font_color='black')

  # 自定义图例
  legend_handles = [
      mlines.Line2D([], [],
                    marker='o',
                    color='blue',
                    linestyle='None',
                    markersize=7,
                    label='Core Nodes'),
      mlines.Line2D([], [],
                    marker='o',
                    color='green',
                    linestyle='None',
                    markersize=10,
                    label='End Nodes'),
      mlines.Line2D([], [], color='gray', linewidth=2, label='Normal Edges'),
      mlines.Line2D([], [], color='red', linewidth=2,
                    label='Important Edges'),
  ]
  plt.legend(handles=legend_handles, scatterpoints=1, loc='best', fontsize=10)

  plt.title(f'IP Hops Network Graph{" for " + domain if domain else ""}',
            fontsize=16)
  plt.savefig(
      f'{output_folder}/IP_Path/IP_Hops_Graph{"_" + domain if domain else ""}.png',
      bbox_inches='tight')
  plt.close()


# def plot_distribution(destination_db, output_folder, field, labels, title):
#   """
#   @brief Plots the distribution of documents based on a specified field.

#   This function calculates the ratio of documents with the specified field in the
#   MongoDB collection and plots a bar chart showing the total number of documents
#   and the number with the field. The plot is saved as a PNG file in the
#   specified output folder.

#   @param destination_db The MongoDB collection object to query the documents from.
#   @param output_folder The folder path where the output plot image will be saved.
#   @param field The field to check for existence or a query dictionary.
#   @param labels The labels for the bar chart.
#   @param title The title of the plot.

#   @return None
#   """
#   logger.info(f'Start to plot {title}')
#   total_docs = destination_db.count_documents({})
#   if isinstance(field, dict):
#     query = field
#   else:
#     query = {field: {"$exists": True}}
#   docs_with_field = destination_db.count_documents(query)
#   ratio = docs_with_field / total_docs * 100

#   fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)
#   ax.bar(labels, [total_docs, docs_with_field])
#   ax.text(labels[0], total_docs, total_docs, ha='center', va='bottom')
#   ax.text(labels[1],
#           docs_with_field,
#           docs_with_field,
#           ha='center',
#           va='bottom')
#   ax.set_xlabel('Document Type')
#   ax.set_ylabel('Number of Documents')
#   ax.set_title(f'{title} (Ratio: {ratio:.2f}%)')
#   fig.savefig(f'{output_folder}/{title.replace(" ", "_")}.png')
#   plt.close(fig)

# def distribution_GFWL_rst_detected(destination_db, output_folder):
#   """
#   @brief Plots the distribution of documents with RST detected.

#   @param destination_db The MongoDB collection object to query the documents from.
#   @param output_folder The folder path where the output plot image will be saved.

#   @return None
#   """
#   plot_distribution(destination_db, output_folder, "rst_detected",
#                     ['Total Docs', 'RST Detected'],
#                     'RST Detected Distribution')

# def distribution_GFWL_redirection_detected(destination_db, output_folder):
#   """
#   @brief Plots the distribution of documents with redirection detected.

#   @param destination_db The MongoDB collection object to query for documents.
#   @param output_folder The folder path where the output plot image will be saved.

#   @return None
#   """
#   plot_distribution(
#       destination_db, output_folder,
#       {"redirection_detected": {
#           "$elemMatch": {
#               "$exists": True
#           }
#       }}, ['Total Docs', 'Redirection Detected'],
#       'Redirection Detected Distribution')

# def distribution_GFWL_Error(destination_db, output_folder):
#   """
#   @brief Plots the error distribution from the given database.

#   This function retrieves documents from the specified database, calculates the error ratio,
#   counts the occurrences of each error, and generates a bar plot showing the distribution
#   of errors. The plot is saved as an image in the specified output folder.

#   @param destination_db The database connection object to retrieve documents from.
#   @param output_folder The folder path where the error distribution plot image will be saved.

#   @return None
#   """
#   logger.info('Start to plot Error Distribution')
#   total_docs = destination_db.count_documents({})
#   docs_with_error = destination_db.count_documents(
#       {"error": {
#           "$elemMatch": {
#               "$exists": True
#           }
#       }})
#   error_ratio = docs_with_error / total_docs * 100

#   error_counts = Counter()
#   docs = destination_db.find({"error": {"$elemMatch": {"$exists": True}}})
#   for doc in docs:
#     for error in doc['error']:
#       error_counts[error] += 1

#   fig, ax = plt.subplots(figsize=(15, 6), constrained_layout=True)
#   ax.bar(error_counts.keys(), error_counts.values())
#   for error, count in error_counts.items():
#     ax.text(error, count, str(count), ha='center', va='bottom')
#   ax.set_xlabel('Error')
#   ax.set_ylabel('Number of Occurrences')
#   ax.set_title(f'Error Distribution (Ratio: {error_ratio:.2f}%)')
#   fig.savefig(f'{output_folder}/Error_Distribution.png', bbox_inches='tight')
#   plt.close(fig)

# def distribution_GFWL_invalid_ip(destination_db, output_folder):
#   """
#   @brief Plots the distribution of valid and invalid IP addresses in the database.

#   This function calculates the ratio of documents with invalid IP addresses to the total number of documents
#   in the specified database. It then generates a bar plot showing the number of valid and invalid IP addresses
#   and saves the plot to the specified output folder.

#   @param destination_db The database connection object to query for documents.
#   @param output_folder The folder path where the plot image will be saved.

#   @return None
#   """
#   plot_distribution(destination_db, output_folder, "invalid_ip",
#                     ['Valid IP', 'Invalid IP'], 'IP Type Distribution')


def ensure_folder_exists(folder):
  if not os.path.exists(folder):
    os.makedirs(folder)


if __name__ == '__main__':
  if os.name == 'posix':
    output_folder = '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic'
  else:
    output_folder = 'D:\\ Developer\\SourceRepo\\GFW-Research\\Pic'

  ensure_folder_exists(output_folder)
  ensure_folder_exists(f'{output_folder}/2024-9')
  ensure_folder_exists(f'{output_folder}/2024-9/IP_Path')
  ensure_folder_exists(f'{output_folder}/2024-11')
  ensure_folder_exists(f'{output_folder}/2024-11/IP_Path')
  ensure_folder_exists(f'{output_folder}/2025-1')
  ensure_folder_exists(f'{output_folder}/2025-1/IP_Path')

  ip_hops_core_path(GFWLocation, f'{output_folder}/2024-9')
  # ip_hops_core_path(adc_db_2024_Nov_GFWL, f'{output_folder}/2024-11')
  # ip_hops_core_path(adc_db_2025_Jan_GFWL, f'{output_folder}/2025-1')

  # with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
  #   # 2024 September Data
  #   executor.submit(distribution_GFWL_rst_detected, GFWLocation,
  #                   f'{output_folder}/2024-9')
  #   executor.submit(distribution_GFWL_redirection_detected, GFWLocation,
  #                   f'{output_folder}/2024-9')
  #   executor.submit(distribution_GFWL_Error, GFWLocation,
  #                   f'{output_folder}/2024-9')
  #   executor.submit(distribution_GFWL_invalid_ip, GFWLocation,
  #                   f'{output_folder}/2024-9')
  #   executor.submit(ip_hops_core_path, GFWLocation, f'{output_folder}/2024-9')

  #   # 2024 November Data
  #   executor.submit(distribution_GFWL_rst_detected, adc_db_2024_Nov_GFWL,
  #                   f'{output_folder}/2024-11')
  #   executor.submit(distribution_GFWL_redirection_detected,
  #                   adc_db_2024_Nov_GFWL, f'{output_folder}/2024-11')
  #   executor.submit(distribution_GFWL_Error, adc_db_2024_Nov_GFWL,
  #                   f'{output_folder}/2024-11')
  #   executor.submit(distribution_GFWL_invalid_ip, adc_db_2024_Nov_GFWL,
  #                   f'{output_folder}/2024-11')
  #   executor.submit(ip_hops_core_path, adc_db_2024_Nov_GFWL,
  #                   f'{output_folder}/2024-11')

  #   # 2025 January Data
  #   executor.submit(distribution_GFWL_rst_detected, adc_db_2025_Jan_GFWL,
  #                   f'{output_folder}/2025-1')
  #   executor.submit(distribution_GFWL_redirection_detected,
  #                   adc_db_2025_Jan_GFWL, f'{output_folder}/2025-1')
  #   executor.submit(distribution_GFWL_Error, adc_db_2025_Jan_GFWL,
  #                   f'{output_folder}/2025-1')
  #   executor.submit(distribution_GFWL_invalid_ip, adc_db_2025_Jan_GFWL,
  #                   f'{output_folder}/2025-1')
  #   executor.submit(ip_hops_core_path, adc_db_2025_Jan_GFWL,
  #                   f'{output_folder}/2025-1')

  # logger.info('All tasks completed.')
