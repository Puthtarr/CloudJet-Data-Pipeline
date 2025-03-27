from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from datetime import datetime, timedelta
import json
from settings import RAW_WEATHER_DATA_DIR, RAW_FLIGHT_DATA_DIR, CLEAN_DATA_DIR

def get_iata_by_city(city):
    mapping = {
        "Tokyo": "NRT",
        "Seoul": "ICN",
        "Singapore": "SIN",
        "Kuala Lumpur": "KUL",
        "Taipei": "TPE",
        "Ho Chi Minh": "SGN",
        "Hong Kong": "HKG"
    }
    return mapping.get(city, None)


def safe_get(d, keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return default
    return d


def float_or_none(value):
    try:
        if isinstance(value, (int, float)):
            return float(value)
        elif isinstance(value, str):
            return float(value.strip())
    except:
        pass
    return None


def transform_data(date):
    day = date.strftime("%Y-%m-%d")
    print(f"Transforming data for: {day}")

    # Create Spark session
    spark = SparkSession.builder.appName('FlightWeatherTransform').getOrCreate()

    # -------------------------
    # ✈️ Load Flight Data
    # -------------------------
    flight_path = RAW_FLIGHT_DATA_DIR / day / "data.json"
    df_flight_raw = spark.read.option("multiline", True).json(str(flight_path))

    df_flight = df_flight_raw.select(
        col("flight.iata").alias("flight_iata"),
        col("departure.iata").alias("departure_iata"),
        col("arrival.iata").alias("arrival_iata"),
        col("arrival.scheduled").alias("arrival_time"),
        col("airline.name").alias("airline"),
        col("arrival.airport").alias("arr_airport"),
    )

    print("Loaded flight data")
    df_flight.show(5)

    # -------------------------
    # 🌦️ Load Weather Data
    # -------------------------
    weather_path = RAW_WEATHER_DATA_DIR / day / "data.json"
    with open(weather_path, "r", encoding="utf-8") as f:
        weather_raw = json.load(f)

    weather_rows = []
    for w in weather_raw:
        weather = safe_get(w, ["data", "weather", 0, "main"])
        if not isinstance(weather, str) or weather.strip() == "":
            weather = "Unknown"

        row = {
            "city": w.get("city"),
            "iata": get_iata_by_city(w.get("city")),
            "temp": float_or_none(safe_get(w, ["data", "main", "temp"])),
            "humidity": float_or_none(safe_get(w, ["data", "main", "humidity"])),
            "weather": weather,
            "wind_speed": float_or_none(safe_get(w, ["data", "wind", "speed"]))
        }
        weather_rows.append(row)

    weather_schema = StructType([
        StructField("city", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("temp", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("weather", StringType(), True),
        StructField("wind_speed", FloatType(), True),
    ])

    df_weather = spark.createDataFrame(weather_rows, schema=weather_schema)

    print("Loaded weather data")

    # df_weather.show()
    # ❌ DO NOT USE .show() or .take() here!
    # -----------------------------------------
    # Spark will throw Py4JJavaError on Windows (some JVM+PySpark versions)
    # if trying to display rows via .show() or .take() — even when data is correct.
    # This is likely due to an internal issue with Java Gateway rendering.
    #
    # ✅ Instead, use:
    # - df_weather.printSchema()          # to inspect structure
    # - df_weather.select(...).toPandas() # to preview content safely
    # - df_weather.limit(n).toPandas().to_csv(...) if needed
    #
    # Ref: Spark + Py4J showString issue on Windows (known in community)
    # -----------------------------------------

    df_weather.printSchema()
    df_flight.printSchema()

    # Join Flight + Weather
    df_joined = df_flight.join(df_weather, df_flight.arrival_iata == df_weather.iata, how="left")
    print("Joined flight + weather data")

    print("Joined flight + weather data")
    print(df_joined.printSchema())

    output_path = CLEAN_DATA_DIR / day
    output_path.mkdir(parents=True, exist_ok=True)

    df_joined.write.mode("overwrite").parquet(str(output_path / "flight_weather.parquet"))
    print(f"Saved cleaned data to {output_path / 'flight_weather.parquet'}")

    # Stop Spark session
    spark.stop()
    print("Spark session stopped.")


if __name__ == "__main__":
    date = datetime.now() - timedelta(days=1)
    transform_data(date)