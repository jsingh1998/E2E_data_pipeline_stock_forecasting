import yfinance as yf
from datetime import datetime
from datetime import timedelta
from datetime import date
import pytz
import numpy as np
import pandas as pd
from stockstats import StockDataFrame as sdf
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.models import DAG


# Define DAG function
@task()
def extract_load(name, start_date, stats_list):
    data_date = str(datetime.now(pytz.timezone("America/New_York")).date().strftime('%Y-%m-%d'))
    
    data = yf.download("GOOGL" , start = "2018-01-01" , interval = '1d')

    data = data.loc[~data.index.duplicated(keep='first')]

    stock_df = sdf.retype(data)
    data[stats_list]=stock_df[stats_list]
    data.reset_index(inplace=True)
    data["company_name"] = name
    data["ny_data_date"] = data_date
    data["ny_data_date"] = data["ny_data_date"].apply(pd.to_datetime)
    data["Date"] = data["Date"].apply(pd.to_datetime)
    data.columns = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume',
       'stochrsi', 'macd', 'macds', 'macdh', 'mfi', 'company_name',
       'ny_data_date']

    credentials = service_account.Credentials.from_service_account_file("/Users/susanoo/Desktop/MSBA/career/github_projects/bigquery/js_cloud.json")
    project_id = 'stock-410113'
    table_id = 'stock-410113.stock_google.new_table'
    client = bigquery.Client(credentials=credentials, project=project_id)

    sql = """
    SELECT *
    FROM stock-410113.stock_google.new_table
    WHERE ny_data_date = '{0}'
    """.format(data_date)

    bq_df = client.query(sql).to_dataframe()

    if bq_df.empty:
        job = client.load_table_from_dataframe(data, table_id)
        job.result()
        print("There are {0} rows added/changed".format(len(data)))
    else:
        bq_df.set_index('date', inplace = True)
        bq_df.sort_index(inplace = True)
        
        n1 = data.drop('stochrsi', axis = 1)
        n2 = bq_df.drop('stochrsi', axis =1)

        changes = data[~n1.apply(tuple, 1).isin(n2.apply(tuple, 1))]
        job = client.load_table_from_dataframe(changes, table_id)
        job.result()
        print("There are {0} rows added/changed".format(len(changes)))

# Declare Dag
with DAG(dag_id='extract_and_load_stocks',
         schedule_interval="0 0 * * *",
         start_date=datetime(2024, 1, 1),
         catchup=False,
         tags=['stocks_etl'])\
        as dag:

    extract_and_load = extract_load("GOOGL", "2018-01-01", ['stochrsi', 'macd', 'mfi'])

    extract_and_load