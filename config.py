"""
全国天气气候分析系统 - 统一配置模块
集中管理所有路径、环境变量和常量，消除硬编码
"""
import os

# ============ 项目路径 ============
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
RESULTS_DIR = os.path.join(DATA_DIR, "analysis_results")
SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts")
WEB_DIR = os.path.join(PROJECT_DIR, "web")

# 数据文件路径
RAW_DATA_PATH = os.path.join(DATA_DIR, "weather_raw.csv")
CLEAN_DATA_PATH = os.path.join(DATA_DIR, "weather_clean.csv")
SQLITE_DB_PATH = os.path.join(DATA_DIR, "weather_analysis.db")

# ============ 环境路径 ============
JAVA_HOME = "/usr/lib/jvm/java-8-openjdk-amd64"
HADOOP_HOME = os.path.join(PROJECT_DIR, "hadoop-3.3.6")
SPARK_HOME = os.path.join(PROJECT_DIR, "spark-3.3.4-bin-hadoop3")

# ============ HDFS配置 ============
HDFS_NAMENODE = "hdfs://localhost:9000"
HDFS_DATA_DIR = "/weather/data"
HDFS_DATA_PATH = f"{HDFS_NAMENODE}{HDFS_DATA_DIR}/weather_clean.csv"

# ============ 数据库配置 ============
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "weather_analysis",
    "charset": "utf8mb4"
}

# ============ CSV字段定义 ============
FIELDNAMES = [
    "city", "date", "year", "month", "temp_avg", "temp_max", "temp_min",
    "humidity", "precipitation", "wind_speed", "wind_direction",
    "pressure", "weather", "lat", "lon"
]

# ============ 数据库表定义（统一MySQL和SQLite建表逻辑） ============
TABLE_SCHEMAS = {
    "city_avg_temp": {
        "columns": ["city", "year", "avg_temp"],
        "unique": ["city", "year"],
        "types_mysql": ["VARCHAR(50)", "INT", "FLOAT"],
        "types_sqlite": ["TEXT", "INTEGER", "REAL"],
    },
    "city_temp_extremes": {
        "columns": ["city", "max_temp", "max_temp_date", "min_temp", "min_temp_date"],
        "unique": ["city"],
        "types_mysql": ["VARCHAR(50)", "FLOAT", "VARCHAR(20)", "FLOAT", "VARCHAR(20)"],
        "types_sqlite": ["TEXT", "REAL", "TEXT", "REAL", "TEXT"],
    },
    "monthly_precipitation": {
        "columns": ["city", "year", "month", "precipitation"],
        "unique": ["city", "year", "month"],
        "types_mysql": ["VARCHAR(50)", "INT", "INT", "FLOAT"],
        "types_sqlite": ["TEXT", "INTEGER", "INTEGER", "REAL"],
    },
    "seasonal_comparison": {
        "columns": ["city", "year", "season", "avg_temp", "total_precipitation"],
        "unique": ["city", "year", "season"],
        "types_mysql": ["VARCHAR(50)", "INT", "VARCHAR(10)", "FLOAT", "FLOAT"],
        "types_sqlite": ["TEXT", "INTEGER", "TEXT", "REAL", "REAL"],
    },
    "extreme_weather": {
        "columns": ["city", "avg_extreme_heat_days", "avg_extreme_cold_days", "avg_heavy_rain_days"],
        "unique": ["city"],
        "types_mysql": ["VARCHAR(50)", "FLOAT", "FLOAT", "FLOAT"],
        "types_sqlite": ["TEXT", "REAL", "REAL", "REAL"],
    },
    "city_humidity": {
        "columns": ["city", "year", "avg_humidity"],
        "unique": ["city", "year"],
        "types_mysql": ["VARCHAR(50)", "INT", "FLOAT"],
        "types_sqlite": ["TEXT", "INTEGER", "REAL"],
    },
    "wind_stats": {
        "columns": ["city", "avg_wind_speed", "max_wind_speed", "min_wind_speed"],
        "unique": ["city"],
        "types_mysql": ["VARCHAR(50)", "FLOAT", "FLOAT", "FLOAT"],
        "types_sqlite": ["TEXT", "REAL", "REAL", "REAL"],
    },
}
