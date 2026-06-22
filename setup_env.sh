#!/bin/bash
# 全国天气气候分析系统 - 环境部署脚本
# 在新机器上运行此脚本完成环境配置

set -e

echo "=================================================="
echo "   全国天气气候分析系统 - 环境部署"
echo "=================================================="

# 1. 安装系统依赖
echo ""
echo "[1/5] 安装系统依赖..."
sudo apt-get update
sudo apt-get install -y openjdk-8-jdk openssh-server python3-pip

# 2. 配置SSH免密登录
echo ""
echo "[2/5] 配置SSH免密登录..."
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
    echo "SSH密钥已生成"
else
    echo "SSH密钥已存在，跳过"
fi
sudo service ssh start

# 3. 安装Hadoop
echo ""
echo "[3/5] 安装Hadoop..."
HADOOP_DIR="$HOME/vibe/weatherItem/hadoop-3.3.6"
if [ ! -d "$HADOOP_DIR" ]; then
    mkdir -p "$(dirname "$HADOOP_DIR")"
    cd /tmp
    wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
    tar -xzf hadoop-3.3.6.tar.gz -C "$HOME/vibe/"
    rm -f hadoop-3.3.6.tar.gz
    echo "Hadoop已安装至 $HADOOP_DIR"
else
    echo "Hadoop已存在，跳过"
fi

# 配置Hadoop（伪分布式）
echo "配置Hadoop..."
HADOOP_ETC="$HADOOP_DIR/etc/hadoop"

# hadoop-env.sh
sed -i "s|export JAVA_HOME=.*|export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64|" "$HADOOP_ETC/hadoop-env.sh"

# hdfs-site.xml
cat > "$HADOOP_ETC/hdfs-site.xml" << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://$HADOOP_DIR/tmp/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://$HADOOP_DIR/tmp/dfs/data</value>
  </property>
</configuration>
XMLEOF

# core-site.xml
cat > "$HADOOP_ETC/core-site.xml" << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>$HADOOP_DIR/tmp</value>
  </property>
</configuration>
XMLEOF

# mapred-site.xml
cat > "$HADOOP_ETC/mapred-site.xml" << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
XMLEOF

# yarn-site.xml
cat > "$HADOOP_ETC/yarn-site.xml" << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
XMLEOF

# 配置环境变量
echo "配置环境变量..."
grep -q "HADOOP_HOME" ~/.bashrc || cat >> ~/.bashrc << 'BASHEOF'

# Hadoop Environment
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/vibe/weatherItem/hadoop-3.3.6
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
BASHEOF

source ~/.bashrc

# 格式化并启动HDFS
echo "格式化HDFS..."
$HADOOP_DIR/bin/hdfs namenode -format -force

echo "启动HDFS..."
$HADOOP_DIR/sbin/start-dfs.sh
$HADOOP_DIR/sbin/start-yarn.sh

# 4. 安装Python依赖
echo ""
echo "[4/5] 安装Python依赖..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
pip3 install -r "$SCRIPT_DIR/requirements.txt"

# 复制cloudpickle（Spark依赖）
CLOUDPICKLE_DIR="$SCRIPT_DIR/cloudpickle"
if [ ! -d "$CLOUDPICKLE_DIR" ]; then
    echo "复制cloudpickle依赖..."
    mkdir -p "$CLOUDPICKLE_DIR"
    python3 -c "
import cloudpickle
import os, shutil
src = os.path.dirname(cloudpickle.__file__)
dst = '$CLOUDPICKLE_DIR'
for f in os.listdir(src):
    s = os.path.join(src, f)
    d = os.path.join(dst, f)
    if os.path.isfile(s): shutil.copy2(s, d)
    elif not os.path.exists(d): shutil.copytree(s, d)
" 2>/dev/null || echo "cloudpickle已存在或跳过"
fi

# 5. 安装MySQL（可选）
echo ""
echo "[5/5] MySQL安装（可选）..."
read -p "是否安装MySQL？[y/N] " install_mysql
if [ "$install_mysql" = "y" ] || [ "$install_mysql" = "Y" ]; then
    sudo apt-get install -y mysql-server
    sudo mysql -u root < "$SCRIPT_DIR/sql/init.sql"
    echo "MySQL已安装并初始化"
    echo "请修改 config.py 中的 MYSQL_CONFIG 密码"
else
    echo "跳过MySQL安装，将使用SQLite"
fi

echo ""
echo "=================================================="
echo "   环境部署完成！"
echo "   运行 bash run.sh 启动系统"
echo "=================================================="
