from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# python logic

def python_first_function():
    print(datetime.now())
    


# creating the DAG and calling python logic

default_dag_args = { 
    'start_date': datetime(2022, 9, 1), 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
    'project_id': 1 
}

# definign our dag

with DAG("exercise2-dag", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:
    
    # defining the task of the DAG
    task_0 = PythonOperator(task_id='first_python_task', python_callable = python_first_function)


    # dependencies between tasks >> <<
    #task_0 >> task_1 