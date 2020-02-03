from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

dag = DAG(
    dag_id='testetetwssdfs',
    start_date=datetime(2019, 12, 1),
    schedule_interval=None
)

op1 = DummyOperator(
    task_id='op1',
    dag=dag
)
op2 = DummyOperator(
    task_id='op2',
    dag=dag
)
op3 = DummyOperator(
    task_id='op3',
    dag=dag
)
op4 = DummyOperator(
    task_id='op4',
    dag=dag
)
op5 = DummyOperator(
    task_id='op5',
    dag=dag
)

[op1, op2] >> [op3, op4] >> op5
