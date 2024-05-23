from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task


from datetime import datetime

import requests
import logging
import json

def get_Redshift_connection(autocommit = True):
    hook = PostgresHook(posgres_conn_id = 'redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.curser()

@task
def extract_transform():
    response = requests.get("https://restcountries.com/v3/all")
    countries = response.json()
    records = []
    
    for country in countries:
        name = country["name"]["official"]
        population = country["population"]
        area = country["area"]
        
        records.append([name, population, area])
    
    return records    


@task
def load(schema, table, records):
    logging.info("load started")    
    cur = get_Redshift_connection()   

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.World_info;")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    country varchar(255) primary key,
   population INT,
   area FLOAT   
);""")
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            country = r[0]
            population = r[1]
            area = r[2]
            sql = f"INSERT INTO {schema}.World_info (country, population, area) VALUES (%s, %s, %s)"
            cur.execute(sql, (country, population, area))
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")   
        raise
    logging.info("load done")
    
with DAG(
    dag_id = 'world_info',
    start_date = datetime(2024,5,23),
    catchup=False,
    tags = ['API'],
    schedule = '30 6 * * 6' #매주 토요일 6시 30분에 실행
) as dag:

    url = Variable.get("https://restcountries.com/v3/all")
    schema = 'chu44200'  
    table = 'World_info'
    
    results =extract_transform()
    load(schema, table, results)