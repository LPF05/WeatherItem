"""
全国天气气候分析系统 - Flask Web后端
提供API接口供前端ECharts调用
优化：添加数据缓存、统一错误处理、使用配置模块
"""
import json
import os
import time
from functools import lru_cache

from flask import Flask, jsonify, render_template

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import RESULTS_DIR, SQLITE_DB_PATH

app = Flask(__name__)

# ============ 数据缓存层 ============
# 启动时一次性加载所有数据到内存，避免每次请求读磁盘
_data_cache = {}
_cache_timestamp = 0
CACHE_TTL = 300  # 缓存有效期5分钟，支持数据更新后自动刷新


def _load_all_data():
    """一次性加载所有分析结果到内存缓存"""
    global _data_cache, _cache_timestamp

    now = time.time()
    if _data_cache and (now - _cache_timestamp) < CACHE_TTL:
        return _data_cache

    json_files = [
        "city_avg_temp", "city_temp_extremes", "monthly_precipitation",
        "seasonal_comparison", "extreme_weather", "city_humidity", "wind_stats",
        "temp_trend", "precipitation_trend", "annual_extreme_trend"
    ]

    for name in json_files:
        path = os.path.join(RESULTS_DIR, f"{name}.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                _data_cache[name] = json.load(f)
        else:
            _data_cache[name] = []

    _cache_timestamp = now
    return _data_cache


def get_data(name):
    """从缓存获取指定分析结果"""
    data = _load_all_data()
    return data.get(name, [])


# ============ 辅助函数 ============

def _get_cities():
    """获取城市列表"""
    data = get_data("city_avg_temp")
    return sorted(set(item["city"] for item in data))


def _get_years():
    """获取年份列表"""
    data = get_data("city_avg_temp")
    return sorted(set(item["year"] for item in data))


# ============ 页面路由 ============

@app.route("/")
def index():
    return render_template("index.html")


# ============ API路由 ============

@app.route("/api/cities")
def api_cities():
    """获取城市列表"""
    return jsonify(_get_cities())


@app.route("/api/years")
def api_years():
    """获取年份列表"""
    return jsonify(_get_years())


@app.route("/api/city_avg_temp")
def api_city_avg_temp():
    """各城市年平均温度"""
    return jsonify(get_data("city_avg_temp"))


@app.route("/api/city_avg_temp/<city>")
def api_city_avg_temp_detail(city):
    """指定城市的年平均温度趋势"""
    data = [item for item in get_data("city_avg_temp") if item["city"] == city]
    return jsonify(data)


@app.route("/api/city_temp_extremes")
def api_city_temp_extremes():
    """各城市最高/最低温记录"""
    return jsonify(get_data("city_temp_extremes"))


@app.route("/api/monthly_precipitation")
def api_monthly_precipitation():
    """月降水量分布"""
    return jsonify(get_data("monthly_precipitation"))


@app.route("/api/monthly_precipitation/<city>/<int:year>")
def api_monthly_precipitation_detail(city, year):
    """指定城市和年份的月降水量"""
    data = [item for item in get_data("monthly_precipitation")
            if item["city"] == city and item["year"] == year]
    return jsonify(data)


@app.route("/api/seasonal_comparison")
def api_seasonal_comparison():
    """季节对比分析"""
    return jsonify(get_data("seasonal_comparison"))


@app.route("/api/seasonal_comparison/<city>")
def api_seasonal_comparison_detail(city):
    """指定城市的季节对比"""
    data = [item for item in get_data("seasonal_comparison") if item["city"] == city]
    return jsonify(data)


@app.route("/api/extreme_weather")
def api_extreme_weather():
    """极端天气统计"""
    return jsonify(get_data("extreme_weather"))


@app.route("/api/city_humidity")
def api_city_humidity():
    """各城市年平均湿度"""
    return jsonify(get_data("city_humidity"))


@app.route("/api/wind_stats")
def api_wind_stats():
    """各城市风速统计"""
    return jsonify(get_data("wind_stats"))


@app.route("/api/overview")
def api_overview():
    """总览数据（单接口聚合，减少前端请求次数）"""
    avg_temp = get_data("city_avg_temp")
    extremes = get_data("city_temp_extremes")
    extreme_w = get_data("extreme_weather")

    years = sorted(set(item["year"] for item in avg_temp))
    latest_year = years[-1] if years else 2024
    latest_temps = [item for item in avg_temp if item["year"] == latest_year]

    return jsonify({
        "latest_year": latest_year,
        "city_count": len(set(item["city"] for item in avg_temp)),
        "latest_avg_temps": latest_temps,
        "extremes": extremes,
        "extreme_weather": extreme_w
    })


# ============ 统一聚合API（前端一次请求获取全部数据） ============

@app.route("/api/all")
def api_all():
    """一次性返回所有分析数据，减少HTTP请求开销"""
    return jsonify({
        "cities": _get_cities(),
        "years": _get_years(),
        "city_avg_temp": get_data("city_avg_temp"),
        "city_temp_extremes": get_data("city_temp_extremes"),
        "monthly_precipitation": get_data("monthly_precipitation"),
        "seasonal_comparison": get_data("seasonal_comparison"),
        "extreme_weather": get_data("extreme_weather"),
        "city_humidity": get_data("city_humidity"),
        "wind_stats": get_data("wind_stats"),
        "temp_trend": get_data("temp_trend"),
        "precipitation_trend": get_data("precipitation_trend"),
        "annual_extreme_trend": get_data("annual_extreme_trend"),
    })


if __name__ == "__main__":
    # 启动时预加载数据
    _load_all_data()
    print(f"数据缓存已加载: {len(_data_cache)} 个数据集")
    app.run(host="0.0.0.0", port=5000, debug=True)
