from airflow import DAG
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
from io import  StringIO
from tempfile import NamedTemporaryFile
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from minio_operations import load_data_to_minio, read_data_from_minio


def create_table(hook, query):
    """ Create a PostgreSQL Table.

    Args:
        hook (PostgresHook): Airflow's PostgresHook object used to interact with the PostgreSQL database.
        query (str): DDL command used to define the structure of the table.
    """
    
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            create_table_sql = query
            cursor.execute(create_table_sql)
        conn.commit()

def insert_data(hook, data, create_temp_tables_query, insert_new_records_query):
    """ Insert data into the PostgreSQL database. If records already exist, they will be overwritten.

    Args:
        hook (PostgresHook): Airflow's PostgresHook object used to interact with the PostgreSQL database.
        data (pandas.DataFrame): A Pandas DataFrame containing the data to be inserted into the database.
        create_temp_tables_query (str): SQL query to create temporary tables in the database for intermediate data storing.
        insert_new_records_query (str): SQL query to insert new records into the destination table, or update existing records.
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_temp_tables_query)
            
            with NamedTemporaryFile(mode='w', suffix='.tsv') as tmp_file:
                data.to_csv(tmp_file.name, sep="\t", index=False, header=False)
                tmp_file.flush()
                with open(tmp_file.name, 'r') as f:
                    cursor.copy_expert("COPY temp_table FROM STDIN", f)
            
            # Append record that don't have
            cursor.execute(insert_new_records_query)
            conn.commit()