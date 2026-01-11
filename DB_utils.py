from dotenv import load_dotenv
from mysql.connector import Error
import pandas as pd
import mysql.connector
import logging
import os
import time 

load_dotenv() 

def ingest_db(db_name=None):
    try:
        if db_name:
            conn = mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=db_name,
                allow_local_infile=True,
                auth_plugin='mysql_native_password'
            )
            logging.info(f"Successfully connected to the database: {db_name}")
        else:
            conn = mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                allow_local_infile=True,
                auth_plugin='mysql_native_password'
            )
            logging.info("Connection with MySQL established without specifying a database.")
        return conn 

    except Exception as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None   

def execute_query(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
        logging.info(f"Query executed successfully: {query}")
        
    except Error as e:
        logging.error(f"Error executing query '{query}': {e}")   
        
def create_table_from_csv(conn, csv_file, table_name):
    """
    Create a MySQL table automatically based on the CSV columns and dtypes.
    Only creates the table if it does not exist.
    """
    
    df_sample = pd.read_csv(csv_file, nrows=5)
    
    # Map pandas dtypes to MySQL types
    type_mapping = {
        'int64': 'BIGINT',
        'float64': 'DOUBLE',
        'object': 'VARCHAR(255)',
        'bool': 'TINYINT(1)',
    }

    columns_sql = []
    for col, dtype in df_sample.dtypes.items():
        mysql_type = type_mapping.get(str(dtype), 'VARCHAR(255)')
        col_clean = col.replace(" ", "_").replace("-", "_")  # sanitize column names
        columns_sql.append(f"`{col_clean}` {mysql_type}")

    create_stmt = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n  "
    create_stmt += ",\n  ".join(columns_sql)
    create_stmt += "\n);"

    try:
        cursor = conn.cursor()
        cursor.execute(create_stmt)
        cursor.close()
        print(f"Table `{table_name}` created successfully.")
        
    except Error as e:
        print(f"Error creating table `{table_name}`:", e)
        
    except Exception as e:
        print(f"Unexpected error creating table `{table_name}`:", e)
