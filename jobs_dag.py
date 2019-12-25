from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging


config = {
    'dag_id_1': {'schedule_interval': None, "start_date": datetime(2019, 12, 1), "table_name": "table_name_1"},
    'dag_id_2': {'schedule_interval': None, "start_date": datetime(2019, 12, 1), "table_name": "table_name_2"},
    'dag_id_3': {'schedule_interval': None, "start_date": datetime(2019, 12, 1), "table_name": "table_name_3"}
}


def process_db_table(dag_id, database, **context):
    logging.info('{dag_id} start processing tables in database: {database}'.format(dag_id, database))


dags = {}
for dag_id in config:
    dag = DAG(
        dag_id=dag_id,
        default_args=config[dag_id]
    )

    start_processing_tables_in_db = PythonOperator(
        task_id='start_processing_tables_in_database',
        provide_context=True,
        python_callable=process_db_table,
        op_kwargs=dict(
            dag_id=dag.dag_id,
            database='db_name'
        ),
        dag=dag
    )
    insert_new_row = DummyOperator(
        task_id='insert_new_row',
        dag=dag
    )
    query_the_table = DummyOperator(
        task_id='query_the_table',
        dag=dag
    )
    start_processing_tables_in_db >> insert_new_row >> query_the_table
    dags.update(dag_id=dag)

globals().update(dags)
