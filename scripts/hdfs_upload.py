"""
全国天气气候分析系统 - HDFS上传模块
将清洗后的气象数据上传至HDFS分布式存储
优化：使用配置模块
"""
import subprocess
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import JAVA_HOME, HADOOP_HOME, CLEAN_DATA_PATH, HDFS_DATA_DIR


def run_cmd(cmd):
    """执行shell命令"""
    print(f"执行: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"错误: {result.stderr}")
    else:
        print(result.stdout.strip())
    return result.returncode == 0


def upload_to_hdfs(local_path=None, hdfs_dir=None):
    """将本地数据上传至HDFS"""
    local_path = local_path or CLEAN_DATA_PATH
    hdfs_dir = hdfs_dir or HDFS_DATA_DIR

    # 设置环境变量
    os.environ.update({
        "JAVA_HOME": JAVA_HOME,
        "HADOOP_HOME": HADOOP_HOME,
        "PATH": f"{JAVA_HOME}/bin:{HADOOP_HOME}/bin:{HADOOP_HOME}/sbin:{os.environ.get('PATH', '')}"
    })

    print("=" * 50)
    print("全国天气气候分析系统 - HDFS数据上传")
    print("=" * 50)

    # 检查HDFS是否运行
    if not run_cmd("hdfs dfsadmin -report 2>/dev/null | head -3"):
        print("HDFS未运行，请先启动HDFS: start-dfs.sh")
        return False

    # 创建HDFS目录
    run_cmd(f"hdfs dfs -mkdir -p {hdfs_dir}")

    # 删除旧数据
    run_cmd(f"hdfs dfs -rm -f {hdfs_dir}/weather_clean.csv")

    # 上传数据
    if run_cmd(f"hdfs dfs -put {local_path} {hdfs_dir}/"):
        print(f"\n数据上传成功!")
        print(f"本地文件: {local_path}")
        print(f"HDFS路径: {hdfs_dir}/weather_clean.csv")

        # 验证上传
        print("\n验证HDFS文件:")
        run_cmd(f"hdfs dfs -ls {hdfs_dir}/")
        run_cmd(f"hdfs dfs -cat {hdfs_dir}/weather_clean.csv | head -5")
        return True
    else:
        print("数据上传失败!")
        return False


if __name__ == "__main__":
    if not os.path.exists(CLEAN_DATA_PATH):
        print(f"数据文件不存在: {CLEAN_DATA_PATH}")
        print("请先运行 collect_data.py 生成数据")
        sys.exit(1)

    upload_to_hdfs()
