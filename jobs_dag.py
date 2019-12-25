from airflow import DAG
from airflow.models import Connection, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime
import logging

dag_number = int(Variable.get('dag_number'))
config = {}
for id in range(dag_number):
    config['dag_id_{}'.format(id)] = \
        {"start_date": datetime(2019, 12, 1), "table_name": "table_name_{}".format(id)}


def process_db_table(dag_id, database, **context):
    logging.info('{dag_id} start processing tables in database: {database}'.format(
        dag_id=dag_id,
        database=database
    ))


def check_table_existance(**context):
    return 'skip_table_creation' if True else 'create_table'


for dag_id in config:
    dag = DAG(
        dag_id=dag_id,
        default_args=config[dag_id],
        schedule_interval=None
    )

    start_processing_tables_in_db = PythonOperator(
        task_id='start_processing_tables_in_database',
        provide_context=True,
        python_callable=process_db_table,
        op_kwargs=dict(
            dag_id=dag_id,
            database='db_name'
        ),
        dag=dag
    )
    check_table_exist = BranchPythonOperator(
        task_id='check_table_exist',
        provide_context=True,
        python_callable=check_table_existance,
        dag=dag
    )
    create_table = DummyOperator(
        task_id='create_table',
        dag=dag
    )
    skip_table_creation = DummyOperator(
        task_id='skip_table_creation',
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
    start_processing_tables_in_db >> check_table_exist >> [create_table, skip_table_creation] >> \
        insert_new_row >> query_the_table

    globals().update({dag_id: dag})


