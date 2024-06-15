"""
Сбор информации по ракетам SpaceX
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests
import json
import datetime as dt
import logging
from plugins.PostgreSQL import PostgreSql
from plugins.Config import Config

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'duzhenky',
    'poke_interval': 600
}

with DAG('etl_spacex_rockets',
         description='Сбор информации по ракетам SpaceX',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['etl']
         ) as dag:
    
    start = DummyOperator(task_id='start')

    today = str(dt.date.today())


    def extract_data(date):
        """
        Получаем данные по API SpaceX
        """
        logging.info('Extracting')
        URL = 'https://api.spacexdata.com/v3/rockets'
        
        try:
            r = requests.get(URL)
            if r.status_code == 200:
                data = r.json()
                with open(f'/opt/airflow/data/{date}_rockets.json', 'w') as f:
                    json.dump(data, f)
                logging.info(f'Файл записан (/opt/airflow/data/{date}_rockets.json)')
            else:
                logging.info(f'Файл не записан ({r.status_code}))')
        except requests.exceptions.ConnectionError as e:
            logging.error(e.strerror)


    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        op_args=[today],
        dag=dag,
    )


    def transform_data(date: str):
        """
        Преобразование полученных данных
        """
        logging.info('Transformation')
        try:
            with open(f'/opt/airflow/data/{date}_rockets.json', 'r') as f:
                data = json.load(f)
                data_lst = []
                for i in data:
                    item_id = i.get('id')
                    is_active = i.get('active')
                    cost_per_launch = i.get('cost_per_launch')
                    first_flight = i.get('first_flight')
                    country = i.get('country')
                    company = i.get('company')
                    height_m = i.get('height').get('meters')
                    diameter_m = i.get('diameter').get('meters')
                    mass_kg = i.get('mass').get('kg')
                    description = i.get('description')
                    load_date = date
                    row = (item_id, is_active, cost_per_launch, first_flight, country, 
                        company, height_m, diameter_m, mass_kg, description, load_date)
                    data_lst.append(row)
                logging.info('Данные преобразованы')
                return data_lst
        except FileNotFoundError as e:
            logging.error(e.strerror)
    

    def load_data(data: list):
        """
        Загрузка данных в БД
        """
        logging.info('Loading')
        rockets_table_creating_q = """
            CREATE TABLE IF NOT EXISTS spacex.rockets (
                item_id int,
                is_active boolean,
                cost_per_launch int, 
                first_flight date,
                country varchar(100),
                company varchar(256),
                height_m numeric,
                diameter_m numeric,
                mass_kg int,
                description text,
                load_date date
            );
        """
        rockets_table_truncating_q = """
            TRUNCATE TABLE spacex.rockets;
        """
        rockets_table_inserting_q = """
            INSERT INTO spacex.rockets
                (item_id, is_active, cost_per_launch, 
                first_flight, country, company, height_m, 
                diameter_m, mass_kg, description, load_date) 
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        config = Config()
        postgresql_config = config.get_postgresql_config()
        db = PostgreSql(postgresql_config[0], 
                        postgresql_config[1], 
                        postgresql_config[2], 
                        postgresql_config[3], 
                        postgresql_config[4])
        logging.info('Создание таблицы (если не существует)')
        db.execute_query(rockets_table_creating_q)
        logging.info('Удаление данных')
        db.execute_query(rockets_table_truncating_q)
        logging.info('Запись данных')
        for i in data:
            db.insert_into_rockets(rockets_table_inserting_q, 
                                i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10])


    rockets = transform_data(today)

    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
        op_args=[rockets],
        dag=dag,
    )
    
    end = DummyOperator(task_id='end')

    start >> extract >> load >> end
