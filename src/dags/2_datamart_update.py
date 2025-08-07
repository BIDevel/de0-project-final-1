import pendulum
import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

vertica_conn_id = 'vertica_conn'

business_dt = '{{ ds }}'

args = {
    "owner": "blebla",
    'retries': 1
}

with DAG(
    dag_id = '2_datamart_update',
    default_args=args,
    schedule_interval='@daily',
    start_date=pendulum.parse('2022-10-01'),
    end_date=pendulum.parse('2022-11-02'),
    catchup = True,
    tags=['final', 'datamart_update', 'stg_to_dwh', 'vertica']
) as dag:  
    external_task_sensor = ExternalTaskSensor(
        task_id='external_task_sensor',
        # execution_delta=datetime.timedelta(minutes=2),
        timeout=30,
        retries=2,
        external_dag_id='1_data_import',
        dag=dag
        )
    
    dwh_ddl = SQLExecuteQueryOperator(
        task_id = 'dwh_ddl',
        conn_id = vertica_conn_id,
        database = 'Vertica',
        sql = 'sql/dwh_ddl.sql'
    )

    dwh_load_currencies = SQLExecuteQueryOperator(
        task_id = 'dwh_load_currencies',
        conn_id = vertica_conn_id,
        database = 'Vertica',
        sql = 'sql/dwh_load_currencies.sql'
    )

    dwh_load_transactions = SQLExecuteQueryOperator(
        task_id = 'dwh_load_transactions',
        conn_id = vertica_conn_id,
        database = 'Vertica',
        sql = 'sql/dwh_load_transactions.sql'
    )

    mart_update = SQLExecuteQueryOperator(
        task_id = 'mart_update',
        conn_id = vertica_conn_id,
        database = 'Vertica',
        sql = 'sql/cdm_mart_load.sql'
    )

    end_task = DummyOperator(task_id='end')

    external_task_sensor >> dwh_ddl >> [dwh_load_currencies, dwh_load_transactions] >>  mart_update >> end_task

