import os
import airflow
import pandas as pd
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

def cvs_to_postgress_pandas():
    data = pd.read_csv(file_path("user_purchase.csv"))
    df = pd.DataFrame(data)
    df.rename(columns=
    {
        'InvoiceNo':'invoice_number', 
        'StockCode':'stock_code',
        'Description':'detail',
        'Quantity':'quantity',
        'InvoiceDate':'invoice_date',
        'UnitPrice':'unit_price',
        'CustomerID':'customer_id',
        'Country':'country'
    },
        inplace=True
    )
    print(df)
    # connecting
    pg_hook = PostgresHook(postgress_conn_id='postgres_default').get_conn()
    curr = pg_hook.cursor()
    # inserting to db
    for row in df.itertuples():
        formated_detail = row.detail.replace("'","''")
        print(formated_detail)
        query = "INSERT INTO user_purchase (invoice_number, stock_code , detail , quantity , invoice_date , unit_price , customer_id, country) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}')".format(
            row.invoice_number,
            row.stock_code,
            formated_detail,
            row.quantity,
            row.invoice_date,
            row.unit_price,
            row.customer_id,
            row.country )
        curr.execute(query)
        pg_hook.commit()

# adding creationg of table
createTable = PostgresOperator(task_id = 'create_table',
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

copy_data = PythonOperator(task_id='csv_to_database',
    provide_context=True,
    python_callable=cvs_to_postgress_pandas,
    dag=dag
)

createTable >> copy_data