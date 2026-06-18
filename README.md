# 全国天气气候分析系统

基于 **HDFS + Spark + MySQL + ECharts** 的大数据气象分析平台，通过 Open-Meteo API 采集多城市真实历史气象数据，进行分布式存储、统计分析和可视化展示。

## 系统架构

```
┌─────────────┐    ┌─────────┐    ┌──────────────┐    ┌──────────┐    ┌──────────────────┐
│ Open-Meteo  │───▶│  HDFS   │───▶│  Spark RDD   │───▶│  MySQL/  │───▶│  Flask+ECharts   │
│ 天气API采集  │    │ 分布式存储│    │  统计分析     │    │  SQLite  │    │   可视化展示      │
└─────────────┘    └─────────┘    └──────────────┘    └──────────┘    └──────────────────┘
```

## 目录结构

```
weather-analysis/
├── config.py                          # 统一配置（路径、常量、表结构）
├── run.sh                             # 一键运行脚本
├── README.md                          # 项目说明
│
├── scripts/                           # 核心脚本
│   ├── collect_data.py                # 数据采集（Open-Meteo API）
│   ├── hdfs_upload.py                 # HDFS数据上传
│   ├── spark_analysis.py              # Spark RDD统计分析
│   └── mysql_store.py                 # 数据库存储（MySQL/SQLite双模式）
│
├── sql/                               # SQL脚本
│   └── init.sql                       # MySQL建表初始化
│
├── web/                               # Web可视化
│   ├── app.py                         # Flask后端（API + 数据缓存）
│   └── templates/
│       └── index.html                 # ECharts前端（7个可视化图表）
│
└── data/                              # 数据目录（运行后生成）
    ├── weather_raw.csv                # 原始采集数据
    ├── weather_clean.csv              # 清洗后数据
    ├── weather_analysis.db            # SQLite数据库
    └── analysis_results/              # Spark分析结果（JSON）
        ├── city_avg_temp.json
        ├── city_temp_extremes.json
        ├── monthly_precipitation.json
        ├── seasonal_comparison.json
        ├── extreme_weather.json
        ├── city_humidity.json
        └── wind_stats.json
```

## 环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| Python | 3.8+ | 推荐 3.10+ |
| Java | 8 | OpenJDK 1.8 |
| Hadoop | 3.3.6 | 伪分布式模式 |
| Spark | 3.5.x | PySpark pip 包 |
| MySQL | 5.7+ | 可选，默认使用 SQLite |

## 安装部署

### 1. 安装基础环境

```bash
# Java
sudo apt-get install -y openjdk-8-jdk

# Python 依赖
pip install pyspark flask pymysql requests
```

### 2. 安装 Hadoop（伪分布式）

```bash
cd ~/vibe
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz

# 配置环境变量
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc
echo 'export HADOOP_HOME=~/vibe/hadoop-3.3.6' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

# 配置SSH免密登录
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# 格式化并启动HDFS
hdfs namenode -format
start-dfs.sh
```

### 3. 安装 MySQL（可选）

```bash
sudo apt-get install -y mysql-server
sudo mysql -u root < sql/init.sql
# 修改 config.py 中的 MYSQL_CONFIG 密码
```

## 使用方法

### 一键运行

```bash
cd weather-analysis
bash run.sh
```

### 分步运行

```bash
# 步骤1: 数据采集（从Open-Meteo API获取真实历史数据）
python3 scripts/collect_data.py

# 步骤2: 上传至HDFS（需HDFS已启动）
python3 scripts/hdfs_upload.py

# 步骤3: Spark RDD分析
python3 scripts/spark_analysis.py          # 使用HDFS数据
python3 scripts/spark_analysis.py --local  # 使用本地数据

# 步骤4: 数据库存储
python3 scripts/mysql_store.py

# 步骤5: 启动Web可视化
cd web && python3 app.py
# 访问 http://localhost:5000
```

## 功能模块

### 数据采集（collect_data.py）

- 调用 **Open-Meteo Historical Weather API** 获取真实历史气象数据
- 覆盖 16 个中国主要城市，时间跨度 2020-2024 年
- 采集要素：温度（最高/最低/平均）、湿度、降水量、风速、气压、天气状况
- 支持增量采集（已有数据自动跳过）和 429 限流重试
- 数据清洗：去除缺失值和异常值

### HDFS 存储（hdfs_upload.py）

- 将清洗后的 CSV 数据上传至 HDFS 分布式存储
- 自动创建目录、覆盖旧数据、验证上传结果

### Spark RDD 分析（spark_analysis.py）

基于 Spark RDD 编程模型完成 7 项统计分析：

| 分析项 | 说明 | 输出 |
|--------|------|------|
| 各城市年平均温度 | 按城市和年份分组求平均 | 70 条 |
| 各城市温度极值 | 历史最高/最低温及日期 | 14 条 |
| 月降水量分布 | 按城市/年/月汇总降水 | 840 条 |
| 季节对比分析 | 春夏秋冬温度与降水对比 | 280 条 |
| 极端天气识别 | 高温(>35°C)/低温(<-10°C)/暴雨(>50mm)天数 | 14 条 |
| 各城市年平均湿度 | 按城市和年份分组求平均 | 70 条 |
| 各城市风速统计 | 平均/最大/最小风速 | 14 条 |

### 数据库存储（mysql_store.py）

- 支持 MySQL 和 SQLite 双模式（MySQL 不可用时自动切换 SQLite）
- 统一建表逻辑，基于 `config.py` 中的 `TABLE_SCHEMAS` 动态生成 SQL
- 支持数据去重（INSERT ON DUPLICATE KEY UPDATE / INSERT OR REPLACE）

### Web 可视化（web/）

Flask 后端 + ECharts 前端，提供 7 个可视化图表：

1. **各城市年平均温度趋势** — 折线图/柱状图（可切换城市/年份）
2. **月降水量分布** — 柱状图
3. **各城市温度极值对比** — 分组柱状图
4. **季节温度与降水对比** — 双轴图
5. **极端天气天数统计** — 堆叠柱状图
6. **各城市年平均湿度** — 柱状图
7. **各城市风速统计** — 折线+柱状组合图

API 接口：

| 接口 | 说明 |
|------|------|
| `/api/all` | 聚合接口，一次返回全部数据 |
| `/api/cities` | 城市列表 |
| `/api/years` | 年份列表 |
| `/api/city_avg_temp` | 各城市年平均温度 |
| `/api/city_temp_extremes` | 温度极值记录 |
| `/api/monthly_precipitation` | 月降水量 |
| `/api/seasonal_comparison` | 季节对比 |
| `/api/extreme_weather` | 极端天气统计 |
| `/api/city_humidity` | 年平均湿度 |
| `/api/wind_stats` | 风速统计 |
| `/api/overview` | 总览数据 |

## 技术要点

- **数据采集**：Open-Meteo API（免费、无需Key、支持1940年至今全球历史数据）
- **分布式存储**：HDFS 伪分布式模式
- **计算引擎**：Spark RDD（含 cache/unpersist 优化、合并遍历减少 shuffle）
- **数据存储**：MySQL/SQLite 双模式，统一 Schema 定义
- **可视化**：Flask 内存缓存 + ECharts 按需渲染 + 聚合API减少请求
