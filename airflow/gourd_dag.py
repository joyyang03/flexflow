from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['joyyang@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'gourd_dag', default_args=default_args, schedule_interval=timedelta(days=1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='indexer_epa',
    bash_command='python3 ~/code/gourdnet/gourdnet/indexers/epa_aqs/daily_summary.py ~/data',
    dag=dag)

t2 = BashOperator(
    task_id='chunker_epa',
    bash_command='python3 ~/code/gourdnet/gourdnet/chunkers/build.py epa_aqs ~/data/epa_aqs ~/data',
    retries=3,
    dag=dag)

t2.set_upstream(t1)