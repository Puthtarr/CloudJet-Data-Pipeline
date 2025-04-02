import os
import logging
import duckdb
import pytz
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

def setup_date():
    date = datetime.now(pytz.timezone("Asia/Bangkok")) - timedelta(days=7)
    return date.strftime("%Y-%m-%d")

def load_env():
    load_dotenv()
    required = {
        "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT"),
        "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
        "MINIO_BUCKET": os.getenv("MINIO_BUCKET"),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {missing}")
    return required

def setup_logging(day):
    log_dir = Path("/home/jovyan/work/data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"load_to_postgres_{day}.log"
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    return log_path

def load_parquet_to_df(minio_config, day):
    parquet_pattern = f"s3://{minio_config['MINIO_BUCKET']}/{day}/flight_weather.parquet/*.parquet"
    os.environ["AWS_ACCESS_KEY_ID"] = minio_config["MINIO_ROOT_USER"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = minio_config["MINIO_ROOT_PASSWORD"]

    db = duckdb.connect()
    db.execute("INSTALL httpfs;")
    db.execute("LOAD httpfs;")
    db.execute("SET s3_region='us-east-1';")
    db.execute(f"SET s3_endpoint='{minio_config['MINIO_ENDPOINT']}';")
    db.execute("SET s3_url_style='path';")
    db.execute("SET s3_use_ssl=false;")

    logging.info(f"Reading parquet from: {parquet_pattern}")

    df = db.execute(f"SELECT * FROM '{parquet_pattern}'").fetchdf()

    # 🔄 Set `date` from `arrival_time` instead of script runtime
    df["date"] = pd.to_datetime(df["arrival_time"]).dt.date

    logging.info(f"Loaded {len(df)} records from MinIO")
    return df, db

def write_to_postgres(db, df, pg_config, day):
    pg_conn_str = f"postgresql://{pg_config['POSTGRES_USER']}:{pg_config['POSTGRES_PASSWORD']}@{pg_config['POSTGRES_HOST']}:{pg_config['POSTGRES_PORT']}/{pg_config['POSTGRES_DB']}"

    db.execute("INSTALL postgres;")
    db.execute("LOAD postgres;")
    db.execute(f"ATTACH '{pg_conn_str}' AS postgres (TYPE postgres);")

    db.execute("""
        CREATE TABLE IF NOT EXISTS postgres.public.flight_weather (
            flight_iata TEXT,
            departure_iata TEXT,
            arrival_iata TEXT,
            arrival_time TIMESTAMP,
            airline TEXT,
            arr_airport TEXT,
            city TEXT,
            iata TEXT,
            temp FLOAT,
            humidity FLOAT,
            weather TEXT,
            wind_speed FLOAT,
            date DATE
        );
    """)

    db.execute(f"DELETE FROM postgres.public.flight_weather WHERE date = DATE '{day}';")
    logging.info(f"Deleted old records for date {day}")

    db.register("df_to_insert", df)
    db.execute("""
        INSERT INTO postgres.public.flight_weather
        SELECT * FROM df_to_insert;
    """)
    logging.info("Data successfully written to PostgreSQL")

def main():
    day = setup_date()
    print(f"Processing date: {day}")
    config = load_env()
    setup_logging(day)
    logging.info("Start loading data from MinIO to PostgreSQL")

    df, db = load_parquet_to_df(config, day)
    write_to_postgres(db, df, config, day)

    logging.info("Load to PostgreSQL completed successfully")

def insert_multiple_day(x):
    for i in range(x, -1, -1):
        print(i)

        date = datetime.now(pytz.timezone("Asia/Bangkok")) - timedelta(days=i)
        day = date.strftime('%Y-%m-%d')
        
        print(f"Processing date: {day}")
        config = load_env()
        setup_logging(day)
        logging.info("Start loading data from MinIO to PostgreSQL")
    
        df, db = load_parquet_to_df(config, day)
        write_to_postgres(db, df, config, day)
    
        logging.info("Load to PostgreSQL completed successfully")
        

if __name__ == '__main__':
    insert_multiple_day(7)
    # main()
