import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import List

from dotenv import load_dotenv
load_dotenv()

def import_csvs_to_postgres(folder_path: str, engine: Engine) -> None:
    """
    Import all CSV files in the given folder to a PostgreSQL database.
    """
    file_list = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

    for file in file_list:
        file_path = os.path.join(folder_path, file)
        table_name = file.split('.')[0]
        chunksize = 10000  # Adjust this value based on your memory constraints

        # Read the CSV in chunks and insert them into the database
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            chunk['dataset'] = table_name
            chunk.to_sql(table_name, engine, if_exists='append', index=False)


def merge_postgres_tables(table_names: List[str], engine: Engine, merged_table_name: str) -> None:
    """
    Merge the data from the given PostgreSQL tables into a single table.
    """
    with engine.connect() as conn:
        merged_query = f"""
        SELECT * 
        FROM {' UNION ALL SELECT * FROM '.join([f'"{name}"' for name in table_names])}
        """
        merged_df = pd.read_sql_query(merged_query, conn)
        merged_df.to_sql(merged_table_name, conn, if_exists='replace', index=False)


engine = create_engine("postgresql://postgres:1234@localhost:5432/postgres")

# Import CSV files into PostgreSQL
folder_path = f'{os.getenv("BASE_PATH")}/data'
import_csvs_to_postgres(folder_path, engine)

# Merge the datasets in PostgreSQL
table_names = [file.split('.')[0] for file in os.listdir(folder_path) if file.endswith('.csv')]
merge_postgres_tables(table_names, engine, 'merged_data')
