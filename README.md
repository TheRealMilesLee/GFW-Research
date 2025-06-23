# GFW-Research
## Project Overview
A research project analyzing DNS poisoning, GFW location tracking, and IP blocking patterns across Chinese telecom providers. Contains extensive data visualization and network analysis components.
## Directory Structure
.
├── Lib
│   ├── AfterDomainChange
│   │   ├── China-Mobile
│   │   │   ├── DNSPoisoning
│   │   │   ├── Error
│   │   │   └── GFWLocation
│   │   ├── China-Telecom
│   │   │   ├── DNSPoisoning
│   │   │   ├── GFWDeployed
│   │   │   └── IPBlocking
│   │   └── Problematic-Data
│   │       └── MSI
│   │           ├── DNSPoisoning
│   │           ├── GFWLocation
│   │           └── IPBlocking
│   ├── BeforeDomainChange
│   │   ├── CompareGroup
│   │   │   ├── DNSPoisoning
│   │   │   ├── GFWLocation
│   │   │   └── IPBlocking
│   │   ├── DNSPoisoning
│   │   ├── GFWLocation
│   │   ├── IPBlocking
│   │   └── Mac
│   │       └── IPBlocking
│   ├── China-Mobile-DNSPoisoning
│   ├── China-Telecom-DNSPoisoning
│   ├── ChinaMobile-DNSPoisoning-2025-January
│   ├── ChinaMobile-DNSPoisoning-November
│   └── scripts
├── Pic
│   ├── 2024-11
│   │   ├── DNS_SERVER_DIST
│   │   └── IP_Path
│   ├── 2024-9
│   │   ├── DNS_SERVER_DIST
│   │   └── IP_Path
│   ├── 2025-1
│   │   ├── DNS_SERVER_DIST
│   │   └── IP_Path
│   ├── China-Mobile-DNSPoisoning
│   └── China-Telecom-DNSPoisoning
└── src
    ├── Database
    │   ├── DNSPoisoning
    │   ├── GFWLocation
    │   ├── Graph
    │   └── Helper
    ├── Import
    └── scripts
## Key Files
- `README.md` (this file)
- `readme.md` (alternate README)
- `requirements.txt` (Python dependencies)
- `.gitignore` (version control configuration)
- `LICENSE` (project licensing)
## Dependencies
```txt
Python >=3.8
Pandas
Matplotlib
NetworkX
## License
MIT License (see `LICENSE` file for full text)
## Data
- 401 CSV files containing network analysis data
- 318 PNG files for visualizations
- 136 text files with metadata
- 25 Python scripts for analysis
- 14 sample configuration files
## Notes
- All analysis focuses on DNS poisoning patterns, GFW location tracking, and IP blocking mechanisms
- Data includes time-series analysis from 2024-09 to 2025-01
- Visualization files are organized by date and analysis type

---

## 中文版本

# GFW-Research
## 项目描述
本项目围绕GFW相关技术研究，包含DNS污染分析、网络定位追踪及IP封锁机制等核心模块。项目数据涵盖多时间段网络行为记录，支持可视化分析与数据挖掘。
## 目录结构
.
├── Lib
│   ├── AfterDomainChange
│   │   ├── China-Mobile
│   │   │   ├── DNSPoisoning
│   │   │   ├── Error
│   │   │   └── GFWLocation
│   │   ├── China-Telecom
│   │   │   ├── DNSPoisoning
│   │   │   ├── GFWDeployed
│   │   │   └── IPBlocking
│   │   └── Problematic-Data
│   │       └── MSI
│   │           ├── DNSPoisoning
│   │           ├── GFWLocation
│   │           └── IPBlocking
│   ├── BeforeDomainChange
│   │   ├── CompareGroup
│   │   │   ├── DNSPoisoning
│   │   │   ├── GFWLocation
│   │   │   └── IPBlocking
│   │   ├── DNSPoisoning
│   │   ├── GFWLocation
│   │   ├── IPBlocking
│   │   └── Mac
│   │       └── IPBlocking
│   ├── China-Mobile-DNSPoisoning
│   ├── China-Telecom-DNSPoisoning
│   ├── ChinaMobile-DNSPoisoning-2025-January
│   ├── ChinaMobile-DNSPoisoning-November
│   └── src
│       ├── Database
│       │   ├── DNSPoisoning
│       │   ├── GFWLocation
│       │   ├── Graph
│       │   └── Helper
│       ├── Import
│       ├── scripts
│       └── ...
├── Pic
│   ├── 2024-11
│   │   ├── DNS_SERVER_DIST
│   │   └── IP_Path
│   ├── 2024-9
│   │   ├── DNS_SERVER_DIST
│   │   └── IP_Path
│   ├── 2025-1
│   │   ├── DNS_SERVER_DIST
│   │   └── IP_Path
│   └── ...
└── ...
## 文件类型统计
- `.csv`: 401 个文件
- `.png`: 318 个文件
- `.txt`: 136 个文件
- `.py`: 25 个文件
- `.sample`: 14 个文件
- `.7z`: 5 个文件
- `.master`: 4 个文件
- `.HEAD`: 4 个文件
- `.rev`: 1 个文件
- `.packed-refs`: 1 个文件
- `.md`: 1 个文件
- `.js`: 1 个文件
- `.index`: 1 个文件
- `.in`: 1 个文件
- `.idx`: 1 个文件
- `.exclude`: 1 个文件
- `.description`: 1 个文件
- `.dcf8236c1e0c4e5aa3b292b021a8e380325047`: 1 个文件
- `.config`: 1 个文件
## 重要文件
- `README.md`: 项目文档
- `requirements.txt`: 依赖管理
- `.gitignore`: 版本控制忽略配置
- `LICENSE`: 授权协议
- `__init__.py`: 模块初始化文件
## 主要编程语言
- Python: 25 个文件
- JavaScript: 1 个文件
## 使用说明
1. 安装依赖: `pip install -r requirements.txt`
2. 运行分析脚本: `python scripts/analysis.py`
3. 查看可视化结果: `python scripts/visualize.py`
