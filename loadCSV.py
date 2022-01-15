import airflow
import os
import csv
import psycopg2 #DB API 2.0 compliant PostgreSQL driver
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd


# load csv to GCP SQL

default_args = {
    'owner' :'mac',
    'depends_on_past': False,
    'email': ['mac.gaxiola@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'insert_data', #identifier
    default_args=default_args,
    schedule_interval='@once', #run once
    catchup=False,
    tags=['insertData'],
    start_date=datetime(2022, 1 , 9)
)

# get path for local CSV
def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path

def csv_to_postgres():
    # connecting
    pg_hook = PostgresHook(postgress_conn_id='postgres_default').get_conn()
    curr = pg_hook.cursor()
    # CSV loading table
    with open(file_path("test.csv"),"r") as f:
        next(f)
        #curr.copy_from(f, 'user_purchase', sep=',', null='N/A')
        curr.copy_expert("""COPY user_purchase(invoice_number, stock_code,detail,quantity,invoice_date,unit_price,customer_id,country) FROM STDIN WITH CSV)""",f)
        #cursor.copy_expert('COPY table_name(col1, col2) FROM STDIN WITH HEADER CSV', f)
    pg_hook.commit()

def cvs_to_postgress_pandas():
    data = pd.read_csv(file_path("test.csv"))
    df = pd.DataFrame(data)
    print(df)
    # connecting
    pg_hook = PostgresHook(postgress_conn_id='postgres_default').get_conn()
    curr = pg_hook.cursor()

    for row in df.itertuples():
        curr.execute('''
                INSERT INTO user_purchase (invoice_number, stock_code,detail,quantity,invoice_date,unit_price,customer_id,country)
                VALUES (?,?,?,?,?,?,?,?)
                ''',
                row.invoice_number, 
                row.stock_code,
                row.detail,
                row.quantity,
                row.invoice_date,
                row.unit_price,
                row.customer_id,
                row.country,
                )
    pg_hook.commit()



# adding creationg of table
task1 = PostgresOperator(task_id = 'create_table',
                        sql="""
                        CREATE TABLE IF NOT EXISTS user_purchase (
                            invoice_number VARCHAR(10), 
                            stock_code VARCHAR(20), 
                            detail VARCHAR(1000), 
                            quantity INTEGER, 
                            invoice_date timestamp, 
                            unit_price numeric(8,3), 
                            customer_id INTEGER, 
                            country VARCHAR(20));
                            """,
                            postgres_conn_id= 'postgres_default',
                            autocommit=True,
                            dag= dag)

task2 = PythonOperator(task_id='csv_to_database',
    provide_context=True,
    python_callable=cvs_to_postgress_pandas(),
    dag=dag
)

task1 >> task2