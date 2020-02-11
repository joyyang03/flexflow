from datetime import datetime, timedelta, time

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor


import sys
sys.path.insert(0, '/home/ubuntu/code/')

from gourdnet.airflow import prep
from gourdnet.airflow import fetch
from gourdnet.airflow import clean
from gourdnet.airflow import sort
from gourdnet.airflow import chunk
from gourdnet.airflow import store


import yaml

def load_config(config_file):
    with open(config_file) as file:
        arg_list = yaml.load(file, Loader=yaml.FullLoader)
    file.close()
    return arg_list

path = '/home/ubuntu/code/flexflow/config/data_config.yaml'
args = load_config(path)


def create_dag(dag_id,
               schedule,
               default_args,
               arg,
               n,
               queue):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args
              )
    
    # def prep_data():
    #     prep.prep_airflow(arg,n,path)
    
    def fetch_data():
        fetch.fetch_main_airflow(arg)

    def clean_data():
        clean.clean_main_airflow(arg)

    def sort_data():
        sort.sort_main_airflow(arg)

    def chunk_data():
        chunk.chunk_main_airflow(arg)
    
    def store_data():
        store.store_main_airflow(arg)


    with dag:

        # sensor_prep_task = ExternalTaskSensor(
        #     task_id='dag_sensor', 
        #     external_dag_id = 'prep_dag', 
        #     external_task_id = None, 
        #     mode = 'reschedule'
        # )
        
        start = DummyOperator(
            task_id='start',
            queue = queue
            )

        # prep_task = PythonOperator(
        #     task_id='prep',
        #     python_callable=prep_data,
        #     queue = queue
        #     )

        fetch_task = PythonOperator(
            task_id='fetch',
            python_callable=fetch_data,
            queue = queue
            )
        
        clean_task = PythonOperator(
            task_id='clean',
            python_callable=clean_data,
            queue = queue
            )
        
        sort_task = PythonOperator(
            task_id='sort',
            python_callable=sort_data,
            queue = queue
            )
        
        chunk_task = PythonOperator(
            task_id='chunk',
            python_callable=chunk_data,
            queue = queue
            )

        store_task = PythonOperator(
            task_id='store',
            python_callable=store_data,
            queue = queue
            )
        
        end = DummyOperator(
            task_id='end',
            queue = queue
            )
         
        # start >> prep_task >> fetch_task >> clean_task >> sort_task >> chunk_task >> store_task >> end
        start >> fetch_task >> clean_task >> sort_task >> chunk_task >> store_task >> end

    return dag





for n,arg in enumerate(args['jobs']):
    dag_id = '{}_job'.format(arg['name'])

    default_args = {
        'owner': 'Airflow',
        'depends_on_past': False,
        'start_date': datetime(2020, 2, 8),
        'email': ['joyyang@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
    }

    schedule = arg['schedule']
    queue = arg['queue']
    # schedule = '@daily'
    # if not arg['sorted_root']:
    #     arg = prep.prep_airflow(arg,n,path)

    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  default_args,
                                  arg,
                                  n,
                                  queue)

# if __name__ == '__main__':
#   print(args)