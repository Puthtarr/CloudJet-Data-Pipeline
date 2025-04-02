import os
import json
import pytz
import logging
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

import sys
sys.path.append("/home/jovyan/work")
from settings import RAW_FLIGHT_DATA_DIR, RAW_WEATHER_DATA_DIR, LOGS_DATA_DIR

def load_env():
    load_dotenv()
    return {
        "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT"),
        "MINIO_BUCKET": os.getenv("MINIO_BUCKET"),
        "MINIO_USER": os.getenv("MINIO_ROOT_USER"),
        "MINIO_PASS": os.getenv("MINIO_ROOT_PASSWORD")
    }

def setup_logging(day):
    log_dir = LOGS_DATA_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"transform_{day}.log"
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return log_file

def create_spark(env):
    return SparkSession.builder \
        .appName("CloudJetTransform") \
        .master("spark://spark:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", env["MINIO_ENDPOINT"]) \
        .config("spark.hadoop.fs.s3a.access.key", env["MINIO_USER"]) \
        .config("spark.hadoop.fs.s3a.secret.key", env["MINIO_PASS"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def safe_get(d, keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return default
    return d

def float_or_none(val):
    try:
        return float(val) if val is not None else None
    except:
        return None

def get_iata_by_city(city):
    return {
        "Tokyo": "NRT", "Seoul": "ICN", "Singapore": "SIN", "Kuala Lumpur": "KUL",
        "Taipei": "TPE", "Ho Chi Minh": "SGN", "Hong Kong": "HKG"
    }.get(city)

def transform_data(spark, env, day):
    try:
        flight_path = RAW_FLIGHT_DATA_DIR / day / 'data.json'
        if not flight_path.exists():
            raise FileNotFoundError(f"Missing flight file: {flight_path}")

        logging.info(f"Loading flight data from {flight_path}")
        df_flight_raw = spark.read.option("multiline", True).json(str(flight_path))
        df_flight = df_flight_raw.select(
            col("flight.iata").alias("flight_iata"),
            col("departure.iata").alias("departure_iata"),
            col("arrival.iata").alias("arrival_iata"),
            col("arrival.scheduled").alias("arrival_time"),
            col("airline.name").alias("airline"),
            col("arrival.airport").alias("arr_airport")
        )

        weather_path = RAW_WEATHER_DATA_DIR / day / 'data.json'
        if not weather_path.exists():
            raise FileNotFoundError(f"Missing weather file: {weather_path}")

        logging.info(f"Loading weather data from {weather_path}")
        with open(weather_path, "r", encoding='utf-8') as f:
            weather_raw = json.load(f)

        weather_rows = []
        for w in weather_raw:
            weather = safe_get(w, ["data", "weather", 0, "main"])
            if not isinstance(weather, str):
                weather = "Unknown"
            weather_rows.append({
                "city": w.get("city"),
                "iata": get_iata_by_city(w.get("city")),
                "temp": float_or_none(safe_get(w, ["data", "main", "temp"])),
                "humidity": float_or_none(safe_get(w, ["data", "main", "humidity"])),
                "weather": weather,
                "wind_speed": float_or_none(safe_get(w, ["data", "wind", "speed"]))
            })

        schema = StructType([
            StructField("city", StringType(), True),
            StructField("iata", StringType(), True),
            StructField("temp", FloatType(), True),
            StructField("humidity", FloatType(), True),
            StructField("weather", StringType(), True),
            StructField("wind_speed", FloatType(), True),
        ])
        df_weather = spark.createDataFrame(weather_rows, schema=schema)

        df_joined = df_flight.join(df_weather, df_flight.arrival_iata == df_weather.iata, how="left")
        output_path = f"s3a://{env['MINIO_BUCKET']}/{day}/flight_weather.parquet"
        df_joined.write.mode("overwrite").parquet(output_path)
        logging.info(f"Saved to MinIO: {output_path}")

    except Exception as e:
        logging.error("Error during transformation", exc_info=True)
        raise

    finally:
        spark.stop()
        logging.info("Spark session stopped")

def main():
    day = date.strftime('%Y-%m-%d')
    env = load_env()
    log_path = setup_logging(day)
    print(f"Logging to: {log_path}")
    logging.info("Start transform process")
    logging.info(f"Target Date: {day}")
    spark = create_spark(env)
    transform_data(spark, env, day)

if __name__ == "__main__":
    date = datetime.now(pytz.timezone("Asia/Bangkok"))
    main()
