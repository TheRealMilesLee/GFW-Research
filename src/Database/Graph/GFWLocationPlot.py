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
merge_db_2024_Nov_GFWL = MongoDBHandler(Merged_db["2024_Nov_GFWL"])
merge_db_2025_Jan_GFWL = MongoDBHandler(Merged_db["2025_GFWL"])


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


def plot_reached_dst_distribution(destination_db, output_folder):
  """
  1. 抓取目标数据库中的ips字段, 数组中第一个就是目标IP, 从[1]开始如果出现目标IP或'Reached'字样则视为到达.
  2. 统计成功到达目标的次数, 并绘制直方图.
  """
  query = {"ips": {"$exists": True, "$ne": []}}
  cursor = destination_db.find(query, {"ips": 1})
  reached_dst = 0
  unreached_dst = 0
  total = 0
  labels = ['Reached', 'Unreached']
  field = 'destination_reached'
  for doc in cursor:
    ips_strings = doc.get('ips', [])
    for ips_str in ips_strings:
      ips = [ip.strip() for ip in ips_str.split(';') if ip.strip()]
      if len(ips) <= 1:
        unreached_dst += 1
      elif ips[0] in ips[1:] or 'Reached' in ips:
        reached_dst += 1
      else:
        unreached_dst += 1
      total += 1
  # 绘制饼图
  plt.figure(figsize=(8, 8))
  plt.pie([reached_dst, unreached_dst],
          labels=labels,
          colors=['green', 'red'],
          autopct='%1.1f%%',
          startangle=140)
  plt.title("The distribution of destination reached")

  # 绘制颜色图例，红色为未到达，绿色为到达
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


def plot_rst_detect(destination_db, output_folder):
  """
  1. 抓取目标数据库中的rst_detected字段, 统计出现次数, 并绘制直方图.
  2. 如果数组为空则为Not detected, 如果不为空则为Detected.
  """
  query = {"rst_detected": {"$exists": True}}
  cursor = destination_db.find(query, {"rst_detected": 1})
  rst_detected = 0
  not_detected = 0
  for doc in cursor:
    if doc.get('rst_detected'):
      rst_detected += 1
    else:
      not_detected += 1
  # 绘制饼图
  plt.figure(figsize=(8, 8))
  plt.pie([rst_detected, not_detected],
          labels=['Detected', 'Not detected'],
          colors=['red', 'green'],
          autopct='%1.1f%%',
          startangle=140)
  plt.title("The distribution of RST detected")

  # 绘制颜色图例，红色为检测到，绿色为未检测到
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
                    linestyle='None')
  ],
             loc='best')
  plt.savefig(f'{output_folder}/RST_Detected.png', bbox_inches='tight')
  plt.close()


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

  plot_reached_dst_distribution(GFWLocation, f'{output_folder}/2024-9')
  plot_rst_detect(merge_db_2024_Nov_GFWL, f'{output_folder}/2024-11')
  plot_rst_detect(merge_db_2025_Jan_GFWL, f'{output_folder}/2025-1')
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
