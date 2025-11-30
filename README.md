# GFW-Research

> EN: Research on DNS poisoning, Great Firewall (GFW) path localization, and IP blocking behavior across Chinese network operators. Includes data collection, parsing, statistical modeling, and visualization.
> 中文: 聚焦中国运营商环境下的 DNS 污染、GFW 路径定位与 IP 封锁行为分析，包含数据采集、解析、统计建模与可视化。

## 📌 Overview / 项目概览
- Longitudinal measurement (时间序列测量) across multiple periods (2024-09 → 2025-01)
- Multi-operator comparative datasets (多运营商对比：移动、电信、问题网络段等)
- Structured separation: before / after domain change (域名变更前后对照)
- Automatic generation of DNS poisoning indicators & path inference graphs
- Reproducible scripts for transforming raw CSV → aggregated metrics → plots

## 🗂 Directory Structure / 目录结构
```
Lib/
    AfterDomainChange/        # 域名变更后测量数据
        China-Mobile/           # 中国移动相关数据
        China-Telecom/          # 中国电信相关数据
        Problematic-Data/MSI/   # 问题样本与异常集
    BeforeDomainChange/       # 域名变更前测量数据
        CompareGroup/           # 对照分组 (DNSPoisoning/GFWLocation/IPBlocking)
    China-Mobile-DNSPoisoning/            # 汇总毒化样本
    China-Telecom-DNSPoisoning/
    ChinaMobile-DNSPoisoning-2025-January/
    ChinaMobile-DNSPoisoning-November/
    scripts/                  # 原始或辅助脚本
Pic/                        # 可视化输出 (时间/类型分层)
    2024-9/, 2024-11/, 2025-1/...  # 日期分组
src/
    Database/                 # 结构化处理模块
        DNSPoisoning/           # DNS 污染特征抽取
        GFWLocation/            # 路径与定位分析
        Graph/                  # 图构建逻辑
        Helper/                 # 工具/公共函数
    Import/                   # 数据导入/清洗
    scripts/                  # 主分析/批处理入口
requirements.txt
LICENSE
README.md
```

## 🔑 Key Components / 关键组件
- Data ingestion (CSV 解析) → normalization → enrichment (特征补充)
- Poisoning detection heuristics (污染判定启发式规则)
- Path localization correlation (路径推断相关性计算)
- Visualization pipeline (指标 → 图表输出 PNG)

## 📦 Dependencies / 依赖
Create a virtual environment first:
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```
`requirements.txt` (示例依赖)：
- Python >= 3.8
- pandas
- matplotlib
- networkx
- (optional) seaborn, numpy, tqdm

## ▶️ Quick Start / 快速开始
```bash
git clone <repo-url>
cd GFW-Research
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. 数据汇总示例
python src/scripts/aggregate_dns_poisoning.py --input Lib/BeforeDomainChange/DNSPoisoning --out out/poisoning-summary.csv

# 2. 生成可视化
python src/scripts/plot_poisoning_trend.py --csv out/poisoning-summary.csv --out Pic/2025-1/DNS_SERVER_DIST

# 3. 路径定位分析
python src/scripts/infer_paths.py --input Lib/AfterDomainChange/China-Mobile/GFWLocation --graph out/path-graph.json
```

## 🧪 Data Semantics / 数据语义
| Folder | 描述 (中文) | Meaning (EN) |
|--------|-------------|--------------|
| DNSPoisoning | DNS 响应异常/投毒样本 | Poisoned / spoofed DNS responses |
| GFWLocation | 通过 TTL/路径推断位置 | Path localization inference |
| IPBlocking | IP 层面连接阻断证据 | IP level blocking indicators |
| CompareGroup | 不同阶段/运营商对比集 | Comparative grouped datasets |

## 🧬 Methodology / 方法简介
1. Passive & active collection (被动+主动探测结合)
2. Classification heuristics (TTL 异常、响应延迟、内容不一致)
3. Cross-operator differential analysis (跨运营商差异统计)
4. Temporal trend modeling (时间序列趋势拟合)

## 🛠 Scripts / 脚本说明 (示例)
| Script | 作用 | Purpose |
|--------|------|---------|
| aggregate_dns_poisoning.py | 汇总 DNS 污染指标 | Aggregate poisoning metrics |
| plot_poisoning_trend.py | 绘制分布/趋势图 | Plot trends & distributions |
| infer_paths.py | GFW 路径定位推断 | Infer path / location |
| export_graph.py | 导出图结构为 JSON | Export graph for downstream |

## 📊 File Statistics / 文件统计 (概览)
- CSV ≈ 400 份（原始与汇总）
- PNG ≈ 300+（图表与路径图）
- Python 脚本 ≈ 25
- Text/metadata ≈ 100+

## ⚠️ Notes / 注意事项
- Some raw data may contain noise; use `Problematic-Data` for exclusion patterns.
- Large path inferences may require increased recursion limits: `import sys; sys.setrecursionlimit(10000)`
- 请避免在生产或外部网络无授权条件下复现主动探测。

## 🧩 Extensibility / 扩展
可添加新运营商目录：`Lib/AfterDomainChange/<Carrier>/DNSPoisoning` 并复用现有聚合脚本。

## 🔒 Ethics & Compliance / 道德与合规
- 所有测试应遵守当地法律法规与服务协议。
- 禁止对非授权目标进行大规模主动探测。

## 🐛 Troubleshooting / 故障排除
| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| CSV 解析失败 | 分隔符不一致 | 指定 `sep`，或预清洗文件 |
| 图为空 | 数据过滤后为空集 | 放宽过滤条件或检查路径 |
| 内存过高 | 大规模批量载入 | 分块读取：`chunksize=` |

## 📤 Export / 数据导出
最终聚合结果可导出为：
```bash
python src/scripts/export_metrics.py --format parquet --input out/poisoning-summary.csv --out out/poisoning-summary.parquet
```

## 📚 License / 许可证
MIT License （详见 `LICENSE`）

## 🤝 Contributing / 贡献
欢迎提交：
- 新的检测启发式
- 更高质量的路径定位算法
- 数据清洗改进

PR 前请：
```bash
flake8 src || echo "Lint warnings reviewed"
pytest -q || echo "Add tests if failing"
```

## ✨ Roadmap / 规划
- [ ] Add multi-region vantage point comparison
- [ ] Integrate BGP anomalies correlation
- [ ] 自动化日报生成

---
## 中文快速指引
1. 创建虚拟环境并安装依赖
2. 运行聚合脚本生成指标
3. 生成趋势图与路径分析图
4. 导出结构化结果供进一步建模

---
## Citation / 引用
若本项目成果用于学术，请引用：
```
@misc{gfwresearch2025,
    title={GFW-Research: Longitudinal DNS Poisoning and Path Localization Study},
    year={2025},
    howpublished={\url{<repo-url>}}
}
```

