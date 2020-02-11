from datetime import datetime

# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash_operator import BashOperator

import yaml

def load_config(config_file):
    with open(config_file) as file:
        arg_list = yaml.load(file, Loader=yaml.FullLoader)
    file.close()
    return arg_list

args = load_config('/Users/joy/documents/insight_de/flexflow/config/data_config.yaml')

for arg in args['jobs']:
    print(arg)