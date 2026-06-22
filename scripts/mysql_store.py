"""
全国天气气候分析系统 - 数据库存储模块
将Spark分析结果存储至MySQL/SQLite数据库
优化：统一建表逻辑（消除SQL重复）、使用配置模块
"""
# pylint: disable=wrong-import-position
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MYSQL_CONFIG, SQLITE_DB_PATH, RESULTS_DIR, TABLE_SCHEMAS  # noqa: E402

try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False


def get_db_connection(use_mysql=True):
    """获取数据库连接"""
    if use_mysql and MYSQL_AVAILABLE:
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
            print("MySQL连接成功")
            return conn, "mysql"
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"MySQL连接失败: {e}")
            print("切换至SQLite...")

    conn = sqlite3.connect(SQLITE_DB_PATH)
    print("SQLite连接成功")
    return conn, "sqlite"


def _build_create_table_sql(table_name, schema, db_type):
    """根据统一schema定义动态生成建表SQL，消除MySQL/SQLite的重复代码"""
    types = schema["types_mysql"] if db_type == "mysql" else schema["types_sqlite"]
    columns = schema["columns"]
    unique = schema["unique"]

    col_defs = []
    if db_type == "mysql":
        col_defs.append("id INT AUTO_INCREMENT PRIMARY KEY")
    else:
        col_defs.append("id INTEGER PRIMARY KEY AUTOINCREMENT")

    for col, typ in zip(columns, types):
        col_defs.append(f"{col} {typ}")

    # 唯一约束
    uk_name = f"uk_{'_'.join(unique)}"
    uk_cols = ", ".join(unique)
    if db_type == "mysql":
        col_defs.append(f"UNIQUE KEY {uk_name} ({uk_cols})")
        return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)}, UNIQUE({uk_cols}))"


def create_tables(conn, db_type):
    """创建数据表（统一逻辑，不再重复写SQL）"""
    cursor = conn.cursor()
    for table_name, schema in TABLE_SCHEMAS.items():
        sql = _build_create_table_sql(table_name, schema, db_type)
        cursor.execute(sql)
    conn.commit()
    print(f"数据表创建完成 ({len(TABLE_SCHEMAS)} 张表)")


def insert_data(conn, db_type, table_name, data, columns):
    """批量插入数据"""
    cursor = conn.cursor()

    if db_type == "mysql":
        placeholders = ", ".join(["%s"] * len(columns))
        cols = ", ".join(columns)
        update_cols = ", ".join([f"{c}=VALUES({c})" for c in columns if c != "id"])
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_cols}"
    else:
        placeholders = ", ".join(["?"] * len(columns))
        cols = ", ".join(columns)
        sql = f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})"

    values = [tuple(item[col] for col in columns) for item in data]
    cursor.executemany(sql, values)
    conn.commit()
    print(f"  {table_name}: 插入{len(values)}条记录")


def store_results(results_dir=None):
    """将分析结果存入数据库"""
    if results_dir is None:
        results_dir = RESULTS_DIR

    print("=" * 50)
    print("全国天气气候分析系统 - 数据库存储")
    print("=" * 50)

    conn, db_type = get_db_connection(use_mysql=True)
    create_tables(conn, db_type)

    for name, schema in TABLE_SCHEMAS.items():
        json_path = os.path.join(results_dir, f"{name}.json")
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            insert_data(conn, db_type, name, data, schema["columns"])
        else:
            print(f"  跳过 {name}: 文件不存在")

    conn.close()
    print(f"\n数据存储完成 (使用{db_type})")


if __name__ == "__main__":
    store_results()
