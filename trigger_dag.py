from airflow import DAG,
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

dag_number = Variable.get('dag_number', default_var=5)

dag = DAG(
    dag_id='trigger_dag',
    schedule_interval=None
)

check_run_file_exists = FileSensor(
    task_id='check_file_exists',
    filepath='input/run',
    dag=dag
)

trigger_dags = TriggerDagRunOperator(
    trigger_dag_id='trigger_dags',

    dag=dag
)

remove_run_file = BashOperator(
    task_id='remove_run_file',
    bash_command='rm input/run',
    dag=dag
)