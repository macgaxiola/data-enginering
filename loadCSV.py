import airflow
import os
import csv
import psycopg2 #DB API 2.0 compliant PostgreSQL driver
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


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
    pg_hook = PostgresHook(postgress_conn_id='postgres_default')
    get_postgres_connection = PostgresHook(postgress_conn_id='postgres_default').get_conn()
    curr = get_postgres_connection.cursor()
    # CSV loading table

    
    with open(file_path("user_purchase.csv"),"r") as file:
        next(file)
        line = file.readline()
        for line in file:
            line = line.replace('test', 'testZ')
            file.write(line)
        curr.copy_from(file, 'user_purchase', sep=',')
        get_postgres_connection.commit()

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
    python_callable=csv_to_postgres,
    dag=dag
)

task1 >> task2