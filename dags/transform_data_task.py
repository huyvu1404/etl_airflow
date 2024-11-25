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


@task()
def fact_sales(data):
    olist_orders_dataset = pd.read_csv(StringIO(data["olist_orders_dataset"]))
    olist_order_payments_dataset = pd.read_csv(StringIO(data["olist_order_payments_dataset"]))
    olist_order_items_dataset = pd.read_csv(StringIO(data["olist_order_items_dataset"]))
        
    merge_df = pd.merge(
            olist_orders_dataset, 
            olist_order_payments_dataset, 
            on='order_id', 
            how='inner'
        )
    merge_df = pd.merge(
            merge_df, 
            olist_order_items_dataset, 
            on='order_id', 
            how='inner'
        )
    fact_sales_df = merge_df[[
            'order_id', 
            'customer_id', 
            'seller_id', 
            'order_purchase_timestamp', 
            'product_id', 
            'price',
            'payment_value',
            'payment_type', 
            'order_status'
        ]]
    load_data_to_minio(fact_sales_df, fact_sales.__wrapped__.__name__)
    return fact_sales_df  
    
@task()
def dim_products(data):
    olist_products_dataset = pd.read_csv(StringIO(data["olist_products_dataset"]))
    product_category_name_translation = pd.read_csv(StringIO(data["product_category_name_translation"]))
    merge_df = pd.merge(
            olist_products_dataset, 
            product_category_name_translation, 
            on='product_category_name', 
            how='inner'
        )
    selected_df = merge_df[[
            'product_id', 
            'product_category_name_english'
        ]]
    load_data_to_minio(selected_df, dim_products.__wrapped__.__name__)
    return selected_df
    
@task()
def sales_values_by_category(fact_sales, dim_products):
    
    successful_orders = fact_sales[fact_sales['order_status'] == 'delivered']
    successful_orders['order_purchase_timestamp'] = pd.to_datetime(successful_orders['order_purchase_timestamp']).dt.date
    
    daily_sales_products = successful_orders.groupby([
            'order_purchase_timestamp', 
            'product_id']).agg({
                    'payment_value': 'sum',
                    'order_id': pd.Series.nunique  
                }).reset_index()

    daily_sales_products = daily_sales_products.rename(columns={
            'order_purchase_timestamp': 'daily',
            'payment_value': 'sales',
            'order_id': 'bills'
            })
    daily_sales_products['sales'] = np.round(daily_sales_products['sales'], 2)
    daily_sales_products['daily'] = pd.to_datetime(daily_sales_products['daily'])
    daily_sales_products['monthly'] = daily_sales_products['daily'].dt.strftime("%Y-%m")

    merge_df = pd.merge(
            daily_sales_products,
            dim_products[['product_id', 'product_category_name_english']],
            on='product_id',
            how='inner'
        )
    grouped_df = merge_df.groupby(['monthly', 'product_category_name_english']).agg({
            'sales': 'sum',
            'bills': 'sum'
        }).reset_index()
    
    grouped_df = grouped_df.rename(columns = {
            'sales': 'total_sales',
            'bills': 'total_bills',
            'product_category_name_english': 'category'
        })
    grouped_df['values_per_bills'] = np.round(grouped_df['total_sales'] / grouped_df['total_bills'], 2)
    monthly_sales_categories = grouped_df[['monthly', 'category', 'total_sales', 'total_bills', 'values_per_bills']]
    return monthly_sales_categories
        
@task_group(group_id='transform_data')
def transform_data(data):
    sales_values_by_category_df = sales_values_by_category(fact_sales(data), dim_products(data))
    return sales_values_by_category_df   