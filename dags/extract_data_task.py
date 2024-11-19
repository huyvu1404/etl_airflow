from airflow import DAG
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
from io import  StringIO
from tempfile import NamedTemporaryFile
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook


from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def read_data_from_minio(file_name):
    """Read a CSV file from Minio bucket.

    Args:
        file_name (str): Name of the file (without extension) stored in Minio bucket.

    Returns:
        str: The content of the file as a string.
    """
    s3_hook = S3Hook(aws_conn_id='minio_connect')
    file = s3_hook.read_key(
        key=f'raw/{file_name}.csv',
        bucket_name='datalake'
    )
    return file
@task(multiple_outputs=True)

def extract_data(list_names):
    """ Read all CSV files from Minio bucket

    Args:
        list_names (list): List of file names in Minio bucket

    Returns:
        dict: A dictionary contains file name and its content.
    """
    data = {}
    for file_name in list_names:
        file = read_data_from_minio(file_name)
        data[file_name] = file
    return data