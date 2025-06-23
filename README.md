# GFW-Research
GFW-Research is a comprehensive project focused on analyzing and studying the behavior and impact of the Great Firewall of China. It includes data collection, analysis, and visualization tools to understand DNS poisoning, IP blocking, and other network-related phenomena.
## Features and Functionality
- **Data Analysis**: Process and analyze large datasets of DNS and IP information.
- **Visualization**: Generate visual representations of network behavior using charts and graphs.
- **Comparative Analysis**: Compare data across different time periods and network providers.
- **Scripting Support**: Includes Python and JavaScript scripts for automation and data processing.
- **Data Storage**: Utilizes structured databases for efficient data retrieval and management.
## Installation Instructions
### Prerequisites
- macOS or iOS (for iOS/macOS projects)
- Xcode (version 14.3 or later)
- Swift version 5.9 or later
- Python 3.8 or later
- Node.js (for JavaScript support)
### Xcode Setup
1. Open Xcode and select "File > New > Project".
2. Choose "App" under the iOS or macOS section.
3. Set the project name to `GFW-Research` and select Swift as the language.
4. Set the deployment target to iOS 16.4 or macOS 14.4.
5. Add the required dependencies using CocoaPods, Swift Package Manager, or Carthage.
### CocoaPods Setup
```bash
pod install
### Swift Package Manager Setup
1. In Xcode, go to "File > Swift Packages > Add Package Dependency".
2. Enter the repository URL for the project.
3. Select the required packages and versions.
### Carthage Setup
```bash
carthage update --platform iOS
## Usage Examples
### Data Analysis Script
```python
from gfw_research import DNSAnalyzer
analyzer = DNSAnalyzer("data/dns_logs.csv")
results = analyzer.analyze()
print(results)
### Visualization
```python
from gfw_research import Plotter
plotter = Plotter("data/analysis_results.csv")
plotter.plot_network_behavior()
## Project Structure Explanation
```
.
├── Lib
│   ├── AfterDomainChange
│   │   ├── China-Mobile
│   │   │   ├── DNSPoisoning
│   │   │   ├── Error
│   │   │   ├── GFWLocation
│   │   ├── China-Telecom
│   │   │   ├── DNSPoisoning
│   │   │   ├── GFWDeployed
│   │   │   ├── IPBlocking
│   │   ├── Problematic-Data
│   │   │   ├── MSI
│   │   │   │   ├── DNSPoisoning
│   │   │   │   ├── GFWLocation
│   │   │   │   ├── IPBlocking
│   ├── BeforeDomainChange
│   │   ├── CompareGroup
│   │   │   ├── DNSPoisoning
│   │   │   ├── GFWLocation
│   │   │   ├── IPBlocking
│   │   ├── DNSPoisoning
│   │   ├── GFWLocation
│   │   ├── IPBlocking
│   │   ├── Mac
│   │   │   ├── IPBlocking
│   ├── Pic
│   │   ├── 2024-11
│   │   │   ├── DNS_SERVER_DIST
│   │   │   ├── IP_Path
│   │   ├── 2024-9
│   │   │   ├── DNS_SERVER_DIST
│   │   │   ├── IP_Path
│   │   ├── 2025-1
│   │   │   ├── DNS_SERVER_DIST
│   │   │   ├── IP_Path
│   │   ├── China-Mobile-DNSPoisoning
│   │   ├── China-Telecom-DNSPoisoning
│   │   ├── ChinaMobile-DNSPoisoning-2025-January
│   │   ├── ChinaMobile-DNSPoisoning-November
│   ├── src
│   │   ├── Database
│   │   │   ├── DNSPoisoning
│   │   │   ├── GFWLocation
│   │   │   ├── Graph
│   │   │   ├── Helper
│   │   ├── Import
│   │   ├── scripts
## Dependencies and Requirements
- **Python**: 3.8 or later
- **JavaScript**: Node.js (for script execution)
- **Swift**: 5.9 or later
- **CocoaPods**: For iOS/macOS dependency management
- **Swift Package Manager**: For Swift package integration
- **Carthage**: For iOS/macOS dependency management
## Contributing Guidelines
1. Fork the repository and create a new branch for your feature or bug fix.
2. Make your changes and ensure all tests pass.
3. Write clear and concise commit messages.
4. Submit a pull request for review.
## License Information
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 中文版本

# GFW-Research 项目分析报告
## 项目基本信息
- **项目名称**: GFW-Research  
- **项目路径**: GFW-Research  
- **分析时间**: 2025-06-22 16:59:51  
---
## 目录结构
```
.
├── Lib
│   ├── AfterDomainChange
│   │   ├── China-Mobile
│   │   │   ├── DNSPoisoning
│   │   │   ├── Error
│   │   │   ├── GFWLocation
│   │   ├── China-Telecom
│   │   │   ├── DNSPoisoning
│   │   │   ├── GFWDeployed
│   │   │   ├── IPBlocking
│   │   ├── Problematic-Data
│   │   │   ├── MSI
│   │   │   │   ├── DNSPoisoning
│   │   │   │   ├── GFWLocation
│   │   │   │   ├── IPBlocking
│   │
│   ├── BeforeDomainChange
│   │   ├── CompareGroup
│   │   │   ├── DNSPoisoning
│   │   │   ├── GFWLocation
│   │   │   ├── IPBlocking
│   │   ├── DNSPoisoning
│   │   ├── GFWLocation
│   │   ├── IPBlocking
│   │   ├── Mac
│   │   │   ├── IPBlocking
│   │
├── Pic
│   ├── 2024-11
│   │   ├── DNS_SERVER_DIST
│   │   ├── IP_Path
│   │
│   ├── 2024-9
│   │   ├── DNS_SERVER_DIST
│   │   ├── IP_Path
│   │
│   ├── 2025-1
│   │   ├── DNS_SERVER_DIST
│   │   ├── IP_Path
│   │
│   ├── China-Mobile-DNSPoisoning
│   ├── China-Telecom-DNSPoisoning
│   ├── ChinaMobile-DNSPoisoning-2025-January
│   ├── ChinaMobile-DNSPoisoning-November
│
├── src
│   ├── Database
│   │   ├── DNSPoisoning
│   │   ├── GFWLocation
│   │   ├── Graph
│   │   ├── Helper
│   │
│   ├── Import
│   ├── scripts
│
```
---
## 文件类型统计
| 文件类型 | 数量 |
|----------|------|
| `.csv`   | 401  |
| `.png`   | 318  |
| `.txt`   | 136  |
| `.py`    | 25   |
| `.sample`| 14   |
| `.7z`    | 5    |
| `.master`| 4    |
| `.HEAD`  | 4    |
| `.rev`   | 1    |
| `.packed-refs` | 1  |
| `.pack`  | 1    |
| `.md`    | 1    |
| `.js`    | 1    |
| `.index` | 1    |
| `.in`    | 1    |
| `.idx`   | 1    |
| `.exclude` | 1  |
| `.description` | 1 |
| `.dcf8236c1e0c4e5aa3b292b021a8e380325047` | 1 |
| `.config`| 1    |
---
## 重要文件
- `README.md`  
- `readme.md`  
- `requirements.txt`  
- `.gitignore`  
- `LICENSE`  
**其他可能的入口文件**:  
- `__init__.py`  
---
## 主要编程语言
- **Python**: 25 个文件  
- **JavaScript**: 1 个文件  
---
## README
[README.md](/README.md)
