import matplotlib
import numpy as np
import os
import networkx as nx
import logging
import multiprocessing
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
from ..DBOperations import Merged_db, MongoDBHandler, ADC_db

matplotlib.use('Agg')  # 使用非交互式后端

CPU_CORES = multiprocessing.cpu_count()
MAX_WORKERS = max(CPU_CORES * 2, 256)  # Dynamically set workers

# Config Logger
for handler in logging.root.handlers[:]:
  logging.root.removeHandler(handler)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Merged_db constants
GFWLocation = MongoDBHandler(Merged_db["TraceRouteResult"])
adc_db_2025_GFWL = MongoDBHandler(
    ADC_db["ChinaMobile-GFWLocation-2025-January"])
adc_db_2024_Nov_GFWL = MongoDBHandler(
    ADC_db["ChinaMobile-GFWLocation-November"])


def ensure_folder_exists(folder):
  if not os.path.exists(folder):
    os.makedirs(folder)


def ip_hops_core_path(destination_db,
                      output_folder,
                      domain=None,
                      top_n=50,
                      frequency_threshold=25,
                      use_ipv4_only=False):
  G = nx.DiGraph()
  edge_frequency = Counter()
  ip_frequency = Counter()
  hops_list = []

  if destination_db == adc_db_2024_Nov_GFWL or destination_db == adc_db_2025_GFWL or use_ipv4_only:
    query = {"IPv4": {"$exists": True, "$ne": []}}
  else:
    query = {"ips": {"$exists": True, "$ne": []}}
  if domain:
    query["domain"] = domain
  cursor = destination_db.find(query, {"ips": 1, "IPv4": 1, "IPv6": 1})

  for doc in cursor:
    if destination_db == adc_db_2024_Nov_GFWL or destination_db == adc_db_2025_GFWL or use_ipv4_only:
      ips_strings = doc.get('IPv4', [])
    else:
      ips_strings = doc.get('ips', [])
      if not ips_strings:
        ips_strings = doc.get('IPv4', []) + doc.get('IPv6', [])
    for ips_str in ips_strings:
      if destination_db == adc_db_2024_Nov_GFWL or destination_db == adc_db_2025_GFWL or use_ipv4_only:
        ips = [ip.strip() for ip in ips_str.split(',') if ip.strip()]
      else:
        ips = [ip.strip() for ip in ips_str.split(';') if ip.strip()]
      if len(ips) <= 2:
        continue
      if ips[0] == '127.0.0.1':
        ips[0] = '192.168.0.1'  # 统一改为起点
      hops = ips[1:]
      hops_list.append(hops)
      for ip in hops:
        ip_frequency[ip] += 1

  filtered_hops_list = []
  end_nodes = set()
  for hops in hops_list:
    if any(ip_frequency[ip] >= frequency_threshold for ip in hops):
      hops.insert(0, '192.168.0.1')
      filtered_hops_list.append(hops)
      end_nodes.add(hops[-1])
      for i in range(len(hops) - 1):
        edge_frequency[(hops[i], hops[i + 1])] += 1

  for edge, freq in edge_frequency.items():
    if freq >= frequency_threshold:
      G.add_edge(*edge, weight=freq)

  top_edges = sorted(G.edges(data=True),
                     key=lambda x: x[2]['weight'],
                     reverse=True)[:top_n]
  core_nodes = {node for edge in top_edges for node in edge[:2]}
  subG = G.subgraph(core_nodes).copy()
  end_nodes &= set(subG.nodes)

  hop_levels = {'192.168.0.1': 0}
  queue = ['192.168.0.1']
  current_level = 1
  visited = set(['192.168.0.1'])
  while queue:
    next_queue = []
    for node in queue:
      for neighbor in subG.successors(node):
        if neighbor not in hop_levels:
          visited.add(neighbor)
          hop_levels[neighbor] = current_level
          next_queue.append(neighbor)
    queue = next_queue
    current_level += 1

  levels = {}
  for node, level in hop_levels.items():
    if level not in levels:
      levels[level] = []
    levels[level].append(node)

  pos = {}
  for level, nodes in levels.items():
    for i, node in enumerate(nodes):
      pos[node] = (i * 20 - len(nodes) * 10, -level * 40)

  for node in subG.nodes:
    if node not in pos:
      pos[node] = (0, 0)

  def is_too_close(pos1, pos2, min_distance=20):
    return abs(pos1[0] -
               pos2[0]) < min_distance and abs(pos1[1] -
                                               pos2[1]) < min_distance

  for node1, pos1 in pos.items():
    for node2, pos2 in pos.items():
      if node1 != node2 and is_too_close(pos1, pos2):
        if pos1[0] <= pos2[0]:
          pos[node2] = (pos2[0] + 15, pos2[1])
        else:
          pos[node2] = (pos2[0] - 15, pos2[1])
        if pos1[1] <= pos2[1]:
          pos[node2] = (pos2[0], pos2[1] + 15)
        else:
          pos[node2] = (pos2[0], pos2[1] - 15)

  plt.figure(figsize=(20, 7))
  nx.draw_networkx_nodes(subG,
                         pos,
                         nodelist=set(subG.nodes) - end_nodes,
                         node_size=200,
                         node_color='blue',
                         label='Core Nodes')
  nx.draw_networkx_nodes(subG,
                         pos,
                         nodelist=end_nodes,
                         node_size=300,
                         node_color='green',
                         label='End Nodes')
  nx.draw_networkx_edges(subG,
                         pos,
                         edge_color='gray',
                         arrowsize=10,
                         label='Normal Edges')
  nx.draw_networkx_edges(subG,
                         pos,
                         edgelist=[(u, v) for u, v, _ in top_edges],
                         edge_color='red',
                         width=2,
                         label='Frequent Edges')

  for node, (x, y) in pos.items():
    angle = 25
    offset_x = 5
    offset_y = 5
    if node in ['223.120.6.70', '223.119.66.102', '223.120.2.246']:
      offset_x += 15
    elif node in ['192.168.0.1', '192.168.1.1']:
      offset_x -= 15
    elif node in ['100.104.0.1']:
      offset_y += 2
    x += offset_x
    y += offset_y
    if angle > 90:
      angle = 80
    plt.annotate(node, (x, y),
                 textcoords="offset points",
                 xytext=(0, 5),
                 ha='center',
                 va='center',
                 fontsize=8,
                 fontweight='bold',
                 rotation=angle)

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
      mlines.Line2D([], [], color='red', linewidth=2, label='Frequent Edges'),
  ]
  plt.legend(handles=legend_handles, scatterpoints=1, loc='best', fontsize=10)
  plt.title(f'IP Hops Network Graph{" for " + domain if domain else ""}',
            fontsize=16)
  plt.savefig(
      f'{output_folder}/IP_Path/IP_Hops_Graph{"_" + domain if domain else ""}.png',
      bbox_inches='tight')
  plt.close()


def plot_dst_distribution(destination_db, output_folder, use_ipv4_only=False):
  if use_ipv4_only:
    query = {"IPv4": {"$exists": True}}
    cursor = destination_db.find(query, {"IPv4": 1})
  else:
    query = {
        "$or": [{
            "ips": {
                "$exists": True
            }
        }, {
            "IPv4": {
                "$exists": True
            }
        }, {
            "IPv6": {
                "$exists": True
            }
        }]
    }
    cursor = destination_db.find(query, {"ips": 1, "IPv4": 1, "IPv6": 1})

  reached_dst = 0
  unreached_dst = 0
  total = 0
  labels = ['Reached', 'Unreached']
  field = 'destination_reached'
  for doc in cursor:
    if use_ipv4_only:
      ips_strings = doc.get('IPv4', [])
    else:
      ips_strings = doc.get('ips', [])
      if not ips_strings:
        ips_strings = doc.get('IPv4', []) + doc.get('IPv6', [])

    for ips_str in ips_strings:
      ips = [ip.strip() for ip in ips_str.split(',')] if use_ipv4_only else [
          ip.strip() for ip in ips_str.split(';')
      ]
      if len(ips) <= 1:
        unreached_dst += 1
      elif ips[0] in ips[1:] or 'Reached' in ips:
        reached_dst += 1
      else:
        unreached_dst += 1
      total += 1

  if reached_dst + unreached_dst > 0:
    plt.figure(figsize=(8, 8))
    plt.pie([reached_dst, unreached_dst],
            labels=labels,
            colors=['green', 'red'],
            autopct='%1.1f%%',
            startangle=140)
    plt.title("The distribution of destination reached")
    plt.legend(handles=[
        mlines.Line2D([], [],
                      color='green',
                      label='Reached',
                      marker='o',
                      linestyle='None'),
        mlines.Line2D([], [],
                      color='red',
                      label='Unreached',
                      marker='o',
                      linestyle='None')
    ],
               loc='best')
    plt.savefig(f'{output_folder}/{field}.png', bbox_inches='tight')
    plt.close()
  else:
    logger.warning("No data to plot for destination reached distribution.")


def plot_rst_detect(destination_db, output_folder):
  query = {
      "$or": [{
          "rst_detected": {
              "$exists": True
          }
      }, {
          "RST Detected": {
              "$exists": True
          }
      }]
  }
  cursor = destination_db.find(query, {"rst_detected": 1, "RST Detected": 1})
  rst_detected = 0
  not_detected = 0
  occured = 0
  for doc in cursor:
    rst_values = doc.get('rst_detected') or doc.get('RST Detected')
    if not rst_values or all(val is False for val in rst_values):
      not_detected += 1
    elif all(val is True for val in rst_values):
      rst_detected += 1
    else:
      occured += 1

  plt.figure(figsize=(8, 8))
  plt.pie([rst_detected, not_detected, occured],
          labels=['Detected', 'Not detected', 'Occured'],
          colors=['red', 'green', 'yellow'],
          autopct='%1.1f%%',
          startangle=140)
  plt.title("The distribution of RST detected")
  plt.legend(handles=[
      mlines.Line2D([], [],
                    color='red',
                    label='Detected',
                    marker='o',
                    linestyle='None'),
      mlines.Line2D([], [],
                    color='green',
                    label='Not detected',
                    marker='o',
                    linestyle='None'),
      mlines.Line2D([], [],
                    color='yellow',
                    label='Occured',
                    marker='o',
                    linestyle='None')
  ],
             loc='best')
  plt.savefig(f'{output_folder}/RST_Detected.png', bbox_inches='tight')
  plt.close()


if __name__ == '__main__':
  if os.name == 'posix':
    output_folder = '/home/silverhand/Developer/SourceRepo/GFW-Research/Pic'
  else:
    output_folder = 'D:\\Developer\\SourceRepo\\GFW-Research\\Pic'

  subfolders = ['2024-9', '2024-11', '2025-1']
  for subfolder in subfolders:
    folder_path = f'{output_folder}/{subfolder}'
    ensure_folder_exists(folder_path)
    ensure_folder_exists(f'{folder_path}/IP_Path')

  ip_hops_core_path(GFWLocation, f'{output_folder}/2024-9')
  ip_hops_core_path(adc_db_2024_Nov_GFWL,
                    f'{output_folder}/2024-11',
                    use_ipv4_only=True)
  ip_hops_core_path(adc_db_2025_GFWL,
                    f'{output_folder}/2025-1',
                    use_ipv4_only=True)

  plot_dst_distribution(GFWLocation, f'{output_folder}/2024-9')
  plot_dst_distribution(adc_db_2024_Nov_GFWL,
                        f'{output_folder}/2024-11',
                        use_ipv4_only=True)
  plot_dst_distribution(adc_db_2025_GFWL,
                        f'{output_folder}/2025-1',
                        use_ipv4_only=True)

  plot_rst_detect(adc_db_2024_Nov_GFWL, f'{output_folder}/2024-11')
  plot_rst_detect(adc_db_2025_GFWL, f'{output_folder}/2025-1')
  logger.info("All done.")
