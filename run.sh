#!/bin/bash
# 全国天气气候分析系统 - 一键运行脚本

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPTS_DIR="$BASE_DIR/scripts"

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME="$BASE_DIR/hadoop-3.3.6"
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3

echo "=================================================="
echo "   全国天气气候分析系统 - 一键运行"
echo "=================================================="

# 步骤1: 数据采集
echo ""
echo "[步骤1/5] 数据采集..."
python3 "$SCRIPTS_DIR/collect_data.py"
if [ $? -ne 0 ]; then
    echo "错误: 数据采集失败，请检查网络连接"
    exit 1
fi

# 步骤2: HDFS上传（可选，失败不影响后续步骤）
echo ""
echo "[步骤2/5] HDFS数据上传..."
python3 "$SCRIPTS_DIR/hdfs_upload.py"
if [ $? -ne 0 ]; then
    echo "警告: HDFS上传失败，将使用本地文件进行Spark分析"
fi

# 步骤3: Spark分析
echo ""
echo "[步骤3/5] Spark RDD分析..."
python3 "$SCRIPTS_DIR/spark_analysis.py" --local
if [ $? -ne 0 ]; then
    echo "错误: Spark分析失败"
    exit 1
fi

# 步骤4: 数据库存储
echo ""
echo "[步骤4/5] 数据库存储..."
python3 "$SCRIPTS_DIR/mysql_store.py"
if [ $? -ne 0 ]; then
    echo "警告: 数据库存储失败，不影响Web可视化"
fi

# 步骤5: 启动Web服务
echo ""
echo "[步骤5/5] 启动Web可视化服务..."
echo "访问 http://localhost:5000 查看分析结果"
cd "$BASE_DIR/web"
python3 app.py
