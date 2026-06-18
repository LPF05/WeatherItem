-- 全国天气气候分析系统 - MySQL数据库初始化脚本
-- 使用方法: mysql -u root -p < init.sql

CREATE DATABASE IF NOT EXISTS weather_analysis DEFAULT CHARSET utf8mb4;
USE weather_analysis;

-- 各城市年平均温度
DROP TABLE IF EXISTS city_avg_temp;
CREATE TABLE city_avg_temp (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    year INT NOT NULL,
    avg_temp FLOAT,
    UNIQUE KEY uk_city_year (city, year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 各城市最高/最低温记录
DROP TABLE IF EXISTS city_temp_extremes;
CREATE TABLE city_temp_extremes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    max_temp FLOAT,
    max_temp_date VARCHAR(20),
    min_temp FLOAT,
    min_temp_date VARCHAR(20),
    UNIQUE KEY uk_city (city)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 月降水量分布
DROP TABLE IF EXISTS monthly_precipitation;
CREATE TABLE monthly_precipitation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    precipitation FLOAT,
    UNIQUE KEY uk_city_ym (city, year, month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 季节对比
DROP TABLE IF EXISTS seasonal_comparison;
CREATE TABLE seasonal_comparison (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    year INT NOT NULL,
    season VARCHAR(10) NOT NULL,
    avg_temp FLOAT,
    total_precipitation FLOAT,
    UNIQUE KEY uk_city_ys (city, year, season)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 极端天气
DROP TABLE IF EXISTS extreme_weather;
CREATE TABLE extreme_weather (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    extreme_heat_days INT DEFAULT 0,
    extreme_cold_days INT DEFAULT 0,
    heavy_rain_days INT DEFAULT 0,
    UNIQUE KEY uk_city (city)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 年平均湿度
DROP TABLE IF EXISTS city_humidity;
CREATE TABLE city_humidity (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    year INT NOT NULL,
    avg_humidity FLOAT,
    UNIQUE KEY uk_city_year (city, year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 风速统计
DROP TABLE IF EXISTS wind_stats;
CREATE TABLE wind_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    avg_wind_speed FLOAT,
    max_wind_speed FLOAT,
    min_wind_speed FLOAT,
    UNIQUE KEY uk_city (city)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
