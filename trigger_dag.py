from airflow import DAG
from airflow.models import Variable
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta


dag_number = Variable.get('dag_number', default_var=5)
run_flag_path = Variable.get('run_flag_path')


def create_process_results(parent_dag_name, child_dag_name, schedule_interval, start_date):
    subdag = DAG(
        dag_id='{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date
    )

    sensor_triggered_dag = ExternalTaskSensor(
        task_id='sensor_triggered_dag',
        external_dag_id='dag_id_0',
        external_task_id='None',
        execution_date_fn=lambda exec_date: exec_date,
        allowed_states=['success'],
        dag=subdag
    )

    print_result = PythonOperator(
        task_id='print_result',
        python_callable=lambda _: None,
        dag=subdag
    )

    remove_run_file = BashOperator(
        task_id='remove_run_file',
        bash_command='rm $AIRFLOW_HOME/{}'.format(run_flag_path),
        dag=dag
    )

    finished_timestamp = BashOperator(
        task_id='finished_timestamp',
        bash_command='touch finished_{{ ts_nodash }}',
        dag=subdag
    )

    sensor_triggered_dag >> print_result >> remove_run_file >> finished_timestamp

    return subdag


dag = DAG(
    dag_id='trigger_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2019, 12, 27)
)

check_run_file_exists = FileSensor(
    task_id='check_file_exists',
    filepath=run_flag_path,
    fs_conn_id='fs_airflow',
    poke_interval=20,
    dag=dag
)

trigger_dags = TriggerDagRunOperator(
    task_id='trigger_dags',
    trigger_dag_id='dag_id_0',
    dag=dag
)

process_results = SubDagOperator(
    task_id='process_results',
    subdag=create_process_results(parent_dag_name=dag.dag_id,
                                  child_dag_name='process_results',
                                  schedule_interval=dag.schedule_interval,
                                  start_date=dag.start_date
                                  ),
    dag=dag
)

check_run_file_exists >> trigger_dags >> process_results
