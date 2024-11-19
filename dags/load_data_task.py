from airflow import DAG
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
from io import  StringIO
from tempfile import NamedTemporaryFile
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psql_operations import create_table, insert_data

@task()
def load_data(data):
    psql_hook = PostgresHook(postgres_conn_id='psql_connect')
    create_table_sql = """  
                    CREATE TABLE IF NOT EXISTS public.monthly_sales_categories(
                        monthly  character varying,
                        category character varying,
                        total_sales numeric(13,2),
                        total_bills int4,
                        values_per_bills numeric(13,2),
                        primary key (monthly, category)
                        );
                    ALTER TABLE public.monthly_sales_categories REPLICA IDENTITY FULL;
                """
    create_table(psql_hook ,create_table_sql)
    create_temp_table_sql = """
                    CREATE TEMPORARY TABLE temp_table AS SELECT * FROM public.monthly_sales_categories WHERE 1=0;
                """
    insert_new_records_sql = """
                    INSERT INTO public.monthly_sales_categories (monthly, category, total_sales, total_bills, values_per_bills)
                    SELECT monthly, category, total_sales, total_bills, values_per_bills
                    FROM temp_table
                    ON CONFLICT (monthly, category)
                    DO UPDATE 
                    SET total_sales = EXCLUDED.total_sales,
                        total_bills = EXCLUDED.total_bills,
                        values_per_bills = EXCLUDED.values_per_bills;
                """
    insert_data(psql_hook, data, create_temp_table_sql, insert_new_records_sql)
    # Create temp table to store new records
    