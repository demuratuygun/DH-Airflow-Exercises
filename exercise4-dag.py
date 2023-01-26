import time 
import json 
from airflow import DAG 
from airflow.operators.postgres_operator import PostgresOperator 
from datetime import timedelta
from airflow.utils.dates import days_ago

default_args = { 
    'owner': 'airflow',
    'retries': 1, 
    'retry_delay': timedelta(minutes=5)
}

# employee name, age
create_query = """
CREATE TABLE employee(
    ID INT PRIMARY KEY,
    NAME VARCHAR (255) NOT NULL,
    AGE INT NOT NULL
);"""
#insert 3 people
insert_data_query = """
INSERT INTO employee(ID, NAME, AGE) VALUES (123, 'Matti Karttunen', 23);
INSERT INTO employee(ID, NAME, AGE) VALUES (124, 'Karl Jablonski', 32);
INSERT INTO employee(ID, NAME, AGE) VALUES (125, 'Tom B. Erichsen', 27);
"""

#calculate the average age of all employees
calculating_averag_age = """
SELECT AVG(AGE)
FROM employee;
"""


dag_postgres = DAG(dag_id = "postgres_dag_connection", default_args = default_args, schedule_interval = None, start_date = days_ago(1))

create_table = PostgresOperator(task_id = "creation_of_table", sql = create_query, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")
insert_data = PostgresOperator(task_id = "insertion_of_data", sql = insert_data_query, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")
group_data = PostgresOperator(task_id = "calculating_averag_age", sql = calculating_averag_age, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")

create_table >> insert_data >> group_data

