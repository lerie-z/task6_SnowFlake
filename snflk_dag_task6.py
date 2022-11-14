import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeHook


def write_from_csv_to_raw():
    hook = SnowflakeHook(snowflake_conn_id='snflk_conn')
    alch = hook.get_sqlalchemy_engine()

    df = pd.read_csv(Variable.get('ios_apps_csv_path'))
    df.to_sql('raw_table', con=alch, if_exists='append', index=False, chunksize=10000)


with DAG('snowflake_task',
         schedule_interval='@once',
         template_searchpath=Variable.get('snflk_path'),
         catchup=False
         ) as dag:

    csv_to_raw_task = PythonOperator(
        task_id='csv_to_raw',
        python_callable=csv_to_raw
    )

    raw_to_stage_task = SnowflakeOperator(
        task_id='raw_to_stage',
        snowflake_conn_id='snflk_conn',
        sql='raw_to_stage.sql'
    )

    stage_to_final_task = SnowflakeOperator(
        task_id='stage_to_final',
        snowflake_conn_id='snflk_conn',
        sql='stage_to_final.sql'
    )

    csv_to_raw_task >> raw_to_stage_task >> stage_to_final_task