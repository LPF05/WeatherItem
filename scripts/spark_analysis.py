"""
全国天气气候分析系统 - Spark RDD分析模块
基于Spark RDD编程模型完成气象数据统计分析
包含：年均统计、5年长期趋势分析
"""
# pylint: disable=wrong-import-position
import json
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import JAVA_HOME, HADOOP_HOME, HDFS_DATA_PATH, CLEAN_DATA_PATH, RESULTS_DIR  # noqa: E402


def create_spark_session():
    """创建SparkSession"""
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["HADOOP_CONF_DIR"] = os.path.join(HADOOP_HOME, "etc/hadoop")
    os.environ.pop("SPARK_HOME", None)

    spark = SparkSession.builder \
        .appName("WeatherClimateAnalysis") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.hadoop.hadoop.home.dir", HADOOP_HOME) \
        .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_HOME}") \
        .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_HOME}") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data(spark, use_hdfs=True):
    """加载气象数据为RDD"""
    if use_hdfs:
        try:
            rdd = spark.sparkContext.textFile(HDFS_DATA_PATH)
            print("从HDFS加载数据成功")
        except Exception:  # pylint: disable=broad-exception-caught
            print("HDFS加载失败，尝试从本地加载...")
            rdd = spark.sparkContext.textFile(f"file://{CLEAN_DATA_PATH}")
            print("从本地加载数据成功")
    else:
        rdd = spark.sparkContext.textFile(f"file://{CLEAN_DATA_PATH}")
        print("从本地加载数据成功")

    header = rdd.first()
    data_rdd = rdd.filter(lambda line: line != header) \
        .map(lambda line: line.split(",")) \
        .map(lambda fields: {
            "city": fields[0],
            "date": fields[1],
            "year": int(fields[2]),
            "month": int(fields[3]),
            "temp_avg": float(fields[4]),
            "temp_max": float(fields[5]),
            "temp_min": float(fields[6]),
            "humidity": float(fields[7]),
            "precipitation": float(fields[8]),
            "wind_speed": float(fields[9]),
            "wind_direction": fields[10],
            "pressure": float(fields[11]),
            "weather": fields[12],
            "lat": float(fields[13]),
            "lon": float(fields[14])
        })
    return data_rdd


def _avg_reduce(key_rdd):
    """通用平均计算：对 (key, value) RDD 求平均值"""
    return key_rdd \
        .mapValues(lambda v: (v, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: round(v[0] / v[1], 1))


# ============ 基础统计分析 ============

def analysis_city_avg_temp(data_rdd):
    """各城市年平均温度"""
    result = _avg_reduce(
        data_rdd.map(lambda x: ((x["city"], x["year"]), x["temp_avg"]))
    ).map(lambda x: {"city": x[0][0], "year": x[0][1], "avg_temp": x[1]}) \
     .collect()
    return sorted(result, key=lambda x: (x["city"], x["year"]))


def analysis_city_temp_extremes(data_rdd):
    """各城市最高/最低温记录（单次RDD遍历）"""
    result = data_rdd.map(lambda x: (x["city"], (x["temp_max"], x["date"], x["temp_min"], x["date"]))) \
        .reduceByKey(lambda a, b: (
            a[0] if a[0] > b[0] else b[0],
            a[1] if a[0] > b[0] else b[1],
            a[2] if a[2] < b[2] else b[2],
            a[3] if a[2] < b[2] else b[3],
        )) \
        .map(lambda x: {
            "city": x[0],
            "max_temp": x[1][0], "max_temp_date": x[1][1],
            "min_temp": x[1][2], "min_temp_date": x[1][3]
        }) \
        .collect()
    return sorted(result, key=lambda x: x["city"])


def analysis_monthly_precipitation(data_rdd):
    """各城市月降水量分布"""
    result = data_rdd.map(lambda x: ((x["city"], x["year"], x["month"]), x["precipitation"])) \
        .reduceByKey(lambda a, b: round(a + b, 1)) \
        .map(lambda x: {
            "city": x[0][0], "year": x[0][1], "month": x[0][2],
            "precipitation": x[1]
        }) \
        .collect()
    return sorted(result, key=lambda x: (x["city"], x["year"], x["month"]))


def analysis_seasonal_comparison(data_rdd):
    """季节对比分析（春3-5月、夏6-8月、秋9-11月、冬12-2月）"""
    def get_season(month):
        if month <= 2:
            return "冬"
        if month <= 5:
            return "春"
        if month <= 8:
            return "夏"
        if month <= 11:
            return "秋"
        return "冬"

    result = data_rdd.map(lambda x: ((x["city"], x["year"], get_season(x["month"])),
                                     (x["temp_avg"], x["precipitation"], 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
        .map(lambda x: {
            "city": x[0][0], "year": x[0][1], "season": x[0][2],
            "avg_temp": round(x[1][0] / x[1][2], 1),
            "total_precipitation": round(x[1][1], 1)
        }) \
        .collect()
    return sorted(result, key=lambda x: (x["city"], x["year"], x["season"]))


def analysis_extreme_weather(data_rdd):
    """极端天气识别（年均天数，按城市和年份统计后再求年均）"""
    # 先按城市+年份统计每年极端天气天数
    yearly = data_rdd.map(lambda x: ((x["city"], x["year"]), (
        1 if x["temp_max"] > 35 else 0,
        1 if x["temp_min"] < -10 else 0,
        1 if x["precipitation"] > 50 else 0
    ))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
        .collect()

    # 按城市分组求年均
    city_years = {}
    for ((city, _year), (heat, cold, rain)) in yearly:
        if city not in city_years:
            city_years[city] = {"heat": [], "cold": [], "rain": []}
        city_years[city]["heat"].append(heat)
        city_years[city]["cold"].append(cold)
        city_years[city]["rain"].append(rain)

    result = []
    for city, vals in sorted(city_years.items()):
        n = len(vals["heat"])
        result.append({
            "city": city,
            "avg_extreme_heat_days": round(sum(vals["heat"]) / n, 1),
            "avg_extreme_cold_days": round(sum(vals["cold"]) / n, 1),
            "avg_heavy_rain_days": round(sum(vals["rain"]) / n, 1),
        })
    return result


def analysis_city_humidity(data_rdd):
    """各城市年平均湿度"""
    result = _avg_reduce(
        data_rdd.map(lambda x: ((x["city"], x["year"]), x["humidity"]))
    ).map(lambda x: {"city": x[0][0], "year": x[0][1], "avg_humidity": x[1]}) \
     .collect()
    return sorted(result, key=lambda x: (x["city"], x["year"]))


def analysis_wind_stats(data_rdd):
    """各城市风速统计"""
    result = data_rdd.map(lambda x: (x["city"], (x["wind_speed"], x["wind_speed"], x["wind_speed"], 1))) \
        .reduceByKey(lambda a, b: (
            a[0] + b[0],
            max(a[1], b[1]),
            min(a[2], b[2]),
            a[3] + b[3]
        )) \
        .map(lambda x: {
            "city": x[0],
            "avg_wind_speed": round(x[1][0] / x[1][3], 1),
            "max_wind_speed": round(x[1][1], 1),
            "min_wind_speed": round(x[1][2], 1)
        }) \
        .collect()
    return sorted(result, key=lambda x: x["city"])


# ============ 5年长期趋势分析 ============

def analysis_temp_trend(data_rdd):
    """5年温度变化趋势：各城市年均温度的逐年变化"""
    # 复用 city_avg_temp 的逻辑，增加变化量计算
    yearly_temps = _avg_reduce(
        data_rdd.map(lambda x: ((x["city"], x["year"]), x["temp_avg"]))
    ).collect()

    # 按城市分组，计算逐年变化
    city_data = {}
    for (city, year), avg_temp in yearly_temps:
        if city not in city_data:
            city_data[city] = []
        city_data[city].append({"year": year, "avg_temp": avg_temp})

    result = []
    for city in sorted(city_data.keys()):
        years_data = sorted(city_data[city], key=lambda x: x["year"])
        # 计算变化量（相对于前一年）
        for i, item in enumerate(years_data):
            if i == 0:
                item["temp_change"] = 0.0
            else:
                item["temp_change"] = round(item["avg_temp"] - years_data[i-1]["avg_temp"], 1)

        # 计算总变化趋势（首尾温差）
        total_change = round(years_data[-1]["avg_temp"] - years_data[0]["avg_temp"], 1)

        result.append({
            "city": city,
            "trend": years_data,
            "total_change": total_change,
            "start_temp": years_data[0]["avg_temp"],
            "end_temp": years_data[-1]["avg_temp"]
        })
    return result


def analysis_precipitation_trend(data_rdd):
    """5年降水变化趋势：各城市年总降水量的逐年变化"""
    # 按城市+年份汇总年降水量
    yearly_precip = data_rdd.map(lambda x: ((x["city"], x["year"]), x["precipitation"])) \
        .reduceByKey(lambda a, b: round(a + b, 1)) \
        .collect()

    city_data = {}
    for (city, year), total_precip in yearly_precip:
        if city not in city_data:
            city_data[city] = []
        city_data[city].append({"year": year, "total_precipitation": total_precip})

    result = []
    for city in sorted(city_data.keys()):
        years_data = sorted(city_data[city], key=lambda x: x["year"])
        # 计算变化量
        for i, item in enumerate(years_data):
            if i == 0:
                item["precip_change"] = 0.0
            else:
                item["precip_change"] = round(item["total_precipitation"] - years_data[i-1]["total_precipitation"], 1)

        total_change = round(years_data[-1]["total_precipitation"] - years_data[0]["total_precipitation"], 1)

        result.append({
            "city": city,
            "trend": years_data,
            "total_change": total_change,
            "start_precip": years_data[0]["total_precipitation"],
            "end_precip": years_data[-1]["total_precipitation"]
        })
    return result


def analysis_annual_extreme_trend(data_rdd):
    """5年极端天气天数趋势：各城市每年极端天气天数的逐年变化"""
    yearly = data_rdd.map(lambda x: ((x["city"], x["year"]), (
        1 if x["temp_max"] > 35 else 0,
        1 if x["temp_min"] < -10 else 0,
        1 if x["precipitation"] > 50 else 0
    ))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
        .collect()

    city_data = {}
    for (city, year), (heat, cold, rain) in yearly:
        if city not in city_data:
            city_data[city] = []
        city_data[city].append({
            "year": year,
            "extreme_heat_days": heat,
            "extreme_cold_days": cold,
            "heavy_rain_days": rain
        })

    result = []
    for city in sorted(city_data.keys()):
        years_data = sorted(city_data[city], key=lambda x: x["year"])
        result.append({"city": city, "trend": years_data})
    return result


# ============ 主流程 ============

def save_results(results, output_dir):
    """保存分析结果为JSON"""
    os.makedirs(output_dir, exist_ok=True)
    for name, data in results.items():
        path = os.path.join(output_dir, f"{name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  已保存: {path} ({len(data)} 条记录)")


def run_all_analyses(use_hdfs=True):
    """运行所有分析"""
    spark = create_spark_session()

    try:
        data_rdd = load_data(spark, use_hdfs=use_hdfs)
        data_rdd.cache()
        record_count = data_rdd.count()
        print(f"数据总量: {record_count} 条记录")

        print("\n开始分析...")
        results = {}

        print("1. 各城市年平均温度...")
        results["city_avg_temp"] = analysis_city_avg_temp(data_rdd)

        print("2. 各城市最高/最低温记录...")
        results["city_temp_extremes"] = analysis_city_temp_extremes(data_rdd)

        print("3. 各城市月降水量分布...")
        results["monthly_precipitation"] = analysis_monthly_precipitation(data_rdd)

        print("4. 季节对比分析...")
        results["seasonal_comparison"] = analysis_seasonal_comparison(data_rdd)

        print("5. 极端天气识别（年均）...")
        results["extreme_weather"] = analysis_extreme_weather(data_rdd)

        print("6. 各城市年平均湿度...")
        results["city_humidity"] = analysis_city_humidity(data_rdd)

        print("7. 各城市风速统计...")
        results["wind_stats"] = analysis_wind_stats(data_rdd)

        print("8. 5年温度变化趋势...")
        results["temp_trend"] = analysis_temp_trend(data_rdd)

        print("9. 5年降水变化趋势...")
        results["precipitation_trend"] = analysis_precipitation_trend(data_rdd)

        print("10. 5年极端天气趋势...")
        results["annual_extreme_trend"] = analysis_annual_extreme_trend(data_rdd)

        data_rdd.unpersist()

        print("\n保存分析结果...")
        save_results(results, RESULTS_DIR)

        print("\n所有分析完成!")
        return results

    finally:
        spark.stop()


if __name__ == "__main__":
    hdfs_mode = "--local" not in sys.argv
    run_all_analyses(use_hdfs=hdfs_mode)
