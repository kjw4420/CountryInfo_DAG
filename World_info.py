from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task


from datetime import datetime
from datetime import timedelta

import requests
import logging
import json

def get_Redshift_connection(autocommit = True):
    hook = PostgresHook(posgres_conn_id = 'redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.curser()

@task
def extract(url):
    logging.info(datetime.utcnow())
    f = requests.get(url)
    return f.text


@task
def transform(text):
    data = json.loads(text)
    records = []
    for info in data:
      country = info["name"]["official"]
      population = info["population"]
      area = info["area"]
      records.append([country, population, area])
    return records

@task
def load(schema, table, records):
    logging.info("load started")    
    cur = get_Redshift_connection()   

    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
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
    schedule = '30 6 * * 6' #매주 토요일 6시 30분에 실행
) as dag:

    url = Variable.get("https://restcountries.com/v3/all")
    schema = 'chu44200'  
    table = 'World_info'
    
    lines = transform(extract(url))
    load(schema, table, lines)