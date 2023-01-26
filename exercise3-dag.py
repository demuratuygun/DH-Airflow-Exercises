import requests 
import time 
import json 

from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.python import BranchPythonOperator 
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta 
import pandas as pd 
import numpy as np 
import os

#exercise: write a DAG which is able to request market data for a list of stocks.


# python logic

def get_data(**kwargs):
    tickers = kwargs["tickers"]

    with requests.Session() as session:

        for ticker in tickers:
            response = session.get('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='+ticker+'&interval=5min&apikey=demo')
            data = response.json()
            print(next(iter(data["Time Series (5min)"])))


# creating the DAG and calling python logic

default_dag_args = {
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}

# definign our dag

with DAG("exercise3-dag", schedule_interval = None, default_args = default_dag_args) as dag_python:
    
    # defining the task of the DAG
    task_0 = PythonOperator(task_id='first_python_task', python_callable = get_data, op_kwargs = {'tickers' : ["IBM", "TSLA", "AAPL", "NKE", "AMZN"]})

    
    # dependencies between tasks >> <<
    #task_0 >> task_1  