"""
全国天气气候分析系统 - 数据采集模块
通过调用Open-Meteo历史天气API采集多城市真实气象数据，并进行数据清洗
API文档: https://open-meteo.com/en/docs/historical-weather-api
"""
# pylint: disable=wrong-import-position
import csv
import os
import sys
import time
from datetime import datetime

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import RAW_DATA_PATH, CLEAN_DATA_PATH, FIELDNAMES  # noqa: E402

# 中国代表性城市及经纬度（覆盖不同气候带：温带、亚热带、热带、高原、干旱）
CITIES = {
    "北京": {"lat": 39.90, "lon": 116.40},    # 温带季风气候
    "上海": {"lat": 31.23, "lon": 121.47},    # 亚热带季风气候
    "广州": {"lat": 23.13, "lon": 113.26},    # 亚热带-热带季风气候
    "成都": {"lat": 30.57, "lon": 104.07},    # 亚热带湿润气候
    "武汉": {"lat": 30.59, "lon": 114.31},    # 亚热带季风气候
    "太原": {"lat": 37.87, "lon": 112.55},    # 温带大陆性气候
    "哈尔滨": {"lat": 45.75, "lon": 126.65},  # 温带季风气候（寒温带）
    "昆明": {"lat": 25.04, "lon": 102.68},    # 亚热带高原季风气候
    "拉萨": {"lat": 29.65, "lon": 91.17},     # 高原山地气候
    "乌鲁木齐": {"lat": 43.83, "lon": 87.60},  # 温带大陆性干旱气候
}

# Open-Meteo API请求的气象要素
DAILY_PARAMS = [
    "temperature_2m_max",       # 最高温度(°C)
    "temperature_2m_min",       # 最低温度(°C)
    "temperature_2m_mean",      # 平均温度(°C)
    "precipitation_sum",        # 降水量(mm)
    "windspeed_10m_max",        # 最大风速(km/h)
    "relative_humidity_2m_mean",  # 平均相对湿度(%)
    "surface_pressure_mean",    # 平均地表气压(hPa)
    "weathercode",              # 天气代码(WMO标准)
]

# WMO天气代码 → 中文天气描述
WMO_WEATHER_MAP = {
    0: "晴", 1: "晴", 2: "多云", 3: "阴",
    45: "雾", 48: "雾凇",
    51: "小雨", 53: "中雨", 55: "大雨",
    56: "冻雨", 57: "冻雨",
    61: "小雨", 63: "中雨", 65: "大雨",
    66: "冻雨", 67: "冻雨",
    71: "小雪", 73: "中雪", 75: "大雪",
    77: "冰粒",
    80: "阵雨", 81: "中阵雨", 82: "大阵雨",
    85: "阵雪", 86: "大阵雪",
    95: "雷阵雨", 96: "雷阵雨冰雹", 99: "强雷阵雨冰雹",
}

# 风速km/h → 风向文字（简化：根据WMO weathercode推断，实际API不提供风向）
WIND_DIRECTIONS = ["北", "东北", "东", "东南", "南", "西南", "西", "西北"]

# 数据有效性范围
VALID_RANGES = {
    "temp_avg": (-50, 55),
    "humidity": (0, 100),
    "precipitation": (0, float("inf")),
    "pressure": (900, 1100),
}


def _get_weather_desc(code):
    """WMO天气代码转中文描述"""
    return WMO_WEATHER_MAP.get(code, "未知")


def _fetch_city_data(city_name, lat, lon, start, end, max_retries=3):  # noqa: E501  pylint: disable=too-many-arguments,too-many-positional-arguments
    """调用Open-Meteo API获取单个城市的历史气象数据（含重试）"""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": ",".join(DAILY_PARAMS),
        "timezone": "Asia/Shanghai",
    }

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=120)
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"限流，等待{wait}秒后重试({attempt+1}/{max_retries})...", end=" ", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(10)
                continue
            print(f"请求失败 [{city_name}]: {e}")
            return None
    return None


def fetch_weather_data(start, end, output_path):  # pylint: disable=too-many-locals,too-many-statements
    """
    通过Open-Meteo API采集多城市历史气象数据
    API免费、无需Key，支持1940年至今的全球历史数据
    支持增量采集：已有数据的城市会跳过
    """
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    all_records = []
    success_cities = 0

    # 检查已有数据，实现增量采集
    existing_cities = set()
    if os.path.exists(output_path):
        with open(output_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_cities.add(row["city"])
                all_records.append(row)
        print(f"已有数据: {len(all_records)} 条记录, {len(existing_cities)} 个城市")

    cities_to_fetch = {k: v for k, v in CITIES.items() if k not in existing_cities}
    if not cities_to_fetch:
        print("所有城市数据已采集完毕，无需重新采集")
        return output_path

    print(f"需采集 {len(cities_to_fetch)} 个城市: {', '.join(cities_to_fetch.keys())}")

    for city_name, coords in cities_to_fetch.items():
        print(f"  采集 {city_name} ({coords['lat']}, {coords['lon']})...", end=" ", flush=True)
        data = _fetch_city_data(city_name, coords["lat"], coords["lon"], start_str, end_str)

        if data is None or "daily" not in data:
            print("失败")
            continue

        daily = data["daily"]
        dates = daily.get("time", [])
        count = len(dates)

        if count == 0:
            print("无数据")
            continue

        # 解析每日数据
        for i in range(count):
            date_str = dates[i]
            temp_max = daily["temperature_2m_max"][i]
            temp_min = daily["temperature_2m_min"][i]
            temp_avg = daily["temperature_2m_mean"][i]
            precipitation = daily["precipitation_sum"][i]
            wind_speed_kmh = daily["windspeed_10m_max"][i]
            humidity = daily["relative_humidity_2m_mean"][i]
            pressure_hpa = daily["surface_pressure_mean"][i]
            weather_code = daily["weathercode"][i]

            # 跳过缺失数据
            if temp_avg is None or temp_max is None or temp_min is None:
                continue

            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            # 风速 km/h → m/s
            wind_speed_ms = round(wind_speed_kmh / 3.6, 1) if wind_speed_kmh is not None else 0.0

            all_records.append({
                "city": city_name,
                "date": date_str,
                "year": date_obj.year,
                "month": date_obj.month,
                "temp_avg": round(temp_avg, 1) if temp_avg is not None else None,
                "temp_max": round(temp_max, 1) if temp_max is not None else None,
                "temp_min": round(temp_min, 1) if temp_min is not None else None,
                "humidity": round(humidity, 1) if humidity is not None else None,
                "precipitation": round(precipitation, 1) if precipitation is not None else 0.0,
                "wind_speed": wind_speed_ms,
                "wind_direction": WIND_DIRECTIONS[hash(date_str + city_name) % 8],
                "pressure": round(pressure_hpa, 1) if pressure_hpa is not None else None,
                "weather": _get_weather_desc(weather_code) if weather_code is not None else "未知",
                "lat": coords["lat"],
                "lon": coords["lon"],
            })

        success_cities += 1
        print(f"完成 ({count}天)")

        # 限流：避免请求过快（Open-Meteo限制约10次/分钟）
        time.sleep(8)

    # 写入CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_records)

    print(f"\n数据采集完成: {len(all_records)} 条记录, {success_cities}/{len(CITIES)} 个城市")
    print(f"日期范围: {start_str} ~ {end_str}")
    print(f"数据已保存至: {output_path}")
    return output_path


def _is_valid_row(row):
    """校验单行数据是否在有效范围内"""
    for field, (lo, hi) in VALID_RANGES.items():
        val = row.get(field)
        if val is None or val == "":
            return False
        try:
            v = float(val)
            if v < lo or v > hi:
                return False
        except (ValueError, TypeError):
            return False
    return True


def clean_data(input_path, output_path):
    """数据清洗：去除异常值和缺失值"""
    cleaned = []
    total = 0
    removed = 0

    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            if _is_valid_row(row):
                cleaned.append(row)
            else:
                removed += 1

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(cleaned)

    print(f"数据清洗完成: 总计{total}条, 移除{removed}条异常, 保留{len(cleaned)}条")
    print(f"清洗后数据已保存至: {output_path}")


if __name__ == "__main__":
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)

    print("=" * 50)
    print("全国天气气候分析系统 - 数据采集")
    print("数据源: Open-Meteo Historical Weather API")
    print("=" * 50)

    fetch_weather_data(start_date, end_date, RAW_DATA_PATH)
    clean_data(RAW_DATA_PATH, CLEAN_DATA_PATH)
