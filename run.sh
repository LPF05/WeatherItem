#!/bin/bash
# 全国天气气候分析系统 - 一键运行脚本

set -e

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPTS_DIR="$BASE_DIR/scripts"

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/home/lpf05/vibe/weatherItem/hadoop-3.3.6
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3

echo "=================================================="
echo "   全国天气气候分析系统 - 一键运行"
echo "=================================================="

# 步骤1: 数据采集
echo ""
echo "[步骤1/5] 数据采集..."
python3 "$SCRIPTS_DIR/collect_data.py"

# 步骤2: HDFS上传
echo ""
echo "[步骤2/5] HDFS数据上传..."
python3 "$SCRIPTS_DIR/hdfs_upload.py" || echo "HDFS上传失败，将使用本地数据"

# 步骤3: Spark分析
echo ""
echo "[步骤3/5] Spark RDD分析..."
python3 "$SCRIPTS_DIR/spark_analysis.py" --local

# 步骤4: 数据库存储
echo ""
echo "[步骤4/5] 数据库存储..."
python3 "$SCRIPTS_DIR/mysql_store.py"

# 步骤5: 启动Web服务
echo ""
echo "[步骤5/5] 启动Web可视化服务..."
echo "访问 http://localhost:5000 查看分析结果"
cd "$BASE_DIR/web"
python3 app.py
