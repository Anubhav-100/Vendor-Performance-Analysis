import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import time

load_dotenv()

logging.basicConfig(
    filename='logs/db_log.log',             # Log file name
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

def ingest_csv_folder(folder_path="csv_data", engine=engine):
    logging.info("-----Starting ETL process-----")
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            start = time.time()
            table_name = file.replace(".csv", "")
            file_path = os.path.join(folder_path, file)

            try:
                df = pd.read_csv(file_path)
                logging.info(f"Starting ingestion: {file}")

                df.to_sql(
                    name=table_name, con=engine, if_exists="replace",                 
                    index=False, chunksize=5000, method="multi"                   
                )
                
                end = time.time()
                logging.info(
                    f"Ingested {file} into `{table_name}` "
                    f"in {(end - start)/60:.2f} minutes"
                )

            except Exception as e:
                logging.error(f"Error ingesting {file}: {e}")
    logging.info("-----ETL process completed.-----")
