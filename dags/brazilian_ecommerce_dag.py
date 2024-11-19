from airflow import DAG
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
from io import  StringIO
from tempfile import NamedTemporaryFile
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from extract_data_task import extract_data
from load_data_task import load_data
from transform_data_task import transform_data

default_args = {
    'owner':'Huy Vu',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}    
@dag(
    dag_id='brazilian_ecommerce',
    default_args=default_args,
    start_date=datetime(2024,8,29),
    schedule='@monthly',
    catchup=False
)
def brazilian_ecommerce_dag():
    
    list_names = ['olist_orders_dataset', 'olist_order_payments_dataset', 'olist_order_items_dataset', 'olist_products_dataset', 'product_category_name_translation']
    extract = extract_data(list_names)
    transform = transform_data(extract)
    load = load_data(transform)
    load
        
brazilian_ecommerce_dag()   
        
    
