from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta


dag_number = Variable.get('dag_number', default_var=5)
run_flag_path = Variable.get('run_flag_path')

dag = DAG(
    dag_id='trigger_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2019, 12, 27)
)

check_run_file_exists = FileSensor(
    task_id='check_file_exists',
    filepath='input/run',
    fs_conn_id='fs_airflow',
    poke_interval=20,
    dag=dag
)

trigger_dags = TriggerDagRunOperator(
    task_id='trigger_dags',
    trigger_dag_id='dag_id_0',
    dag=dag
)

remove_run_file = BashOperator(
    task_id='remove_run_file',
    bash_command='rm $AIRFLOW_HOME/{}'.format(run_flag_path),
    dag=dag
)

check_run_file_exists >> trigger_dags >> remove_run_file
