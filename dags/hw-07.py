
from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State
import random
import time

GOLD = 'Gold'
SILVER = 'Silver'
BRONZE = 'Bronze'

default_args = {
    'start_date': datetime(2020, 0, 0, 0, 0),
}

CONNECTION = "goit_mysql_db_amaslianko"
TABLE = "olympic_dataset.amaslianko_medals"

def create_calculation_operator(type: str) -> MySqlOperator:
    return MySqlOperator(
        task_id=f'calc_{type}',
        mysql_conn_id=CONNECTION,
        sql=f"""
            INSERT INTO {TABLE} (type, count, created_at)
            SELECT '{type}', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results 
            WHERE medal = '{type}';
        """,
    )

with DAG(
        'hw-07',
        default_args=default_args,
        schedule_interval=None,  #
        catchup=False,
) as dag:
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=CONNECTION,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                    id         int auto_increment primary key,
                    type text      null,
                    count      int       null,
                    created_at timestamp null
                );
            """
    )

    random_medal = PythonOperator(
        task_id='random_medal',
        python_callable= lambda: random.choice([GOLD, SILVER, BRONZE])
    )

    pick_random_task = BranchPythonOperator(
        task_id='pick_random_task',
        python_callable=lambda ti: f"calc_{ti.xcom_pull(task_ids='random_medal')}"
    )

    calc_Gold = create_calculation_operator(GOLD)
    calc_Silver = create_calculation_operator(SILVER)
    calc_Bronze = create_calculation_operator(BRONZE)

    delay = PythonOperator(
        task_id='delay',
        python_callable=lambda: time.sleep(5),
        trigger_rule=tr.ONE_SUCCESS
    )

    validate = SqlSensor(
        task_id='validate',
        conn_id=CONNECTION,
        sql=f"""
            SELECT COUNT(id) 
            FROM {TABLE} 
            WHERE created_at >= NOW() - INTERVAL 30 SECOND;
        """,
        mode="poke",
        poke_interval=5,
        timeout=30
    )

    create_table >> random_medal >> pick_random_task >> [calc_Bronze, calc_Silver, calc_Gold] >> delay >> validate
