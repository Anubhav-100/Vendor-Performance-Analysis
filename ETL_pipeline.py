import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import time

load_dotenv()

logging.basicConfig(
    filename='logs/db_log.log',              # Log file name
    level=logging.INFO,                      # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'                        
)

def connect_mysql():
    try:
        engine = create_engine(
            f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}",
            pool_pre_ping=True
        )
        logging.info("Connected to MySQL server")
        return engine
    except Exception as e:
        logging.error(f"MySQL server connection failed: {e}")
        raise

def connect_db(db_name=None):
    try:
        if not db_name:
            db_name = os.getenv("DB_NAME")
        engine = create_engine(
            f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{db_name}",
            pool_pre_ping=True
        )
        logging.info(f"Connected to MySQL database: {db_name}")
        return engine
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise
    
engine = connect_db()

def ingest_db(df, table_name, engine):
    start = time.time()
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=15000,
            method="multi"
        )
        end = time.time()
        logging.info(
            f"Ingested DataFrame into `{table_name}` in {(end - start)/60:.2f} minutes"
        )
    except Exception as e:
        logging.error(f"Error ingesting `{table_name}`: {e}")

def ingest_raw_data(folder_path="csv_data", engine=engine):
    logging.info("-----------Starting ETL process-----------")

    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            table_name = file.replace(".csv", "")
            file_path = os.path.join(folder_path, file)

            try:
                logging.info(f"Reading CSV file: {file}")
                df = pd.read_csv(file_path)
                ingest_db(df, table_name, engine)  # call the ingestion function
            except Exception as e:
                logging.error(f"Error reading `{file}`: {e}")

    logging.info("-----------ETL process completed.-----------")
