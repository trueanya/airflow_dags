# -*- coding: utf-8 -*-
# The DAG object; we'll need this to instantiate a DAG
import json
from airflow import DAG
from ds_utils import PythonVirtualEnvOperatorCustomPip
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta

# Operators; we need this to operate!
from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.contrib.operators.vertica_operator import VerticaOperator
from utils import dag_name


DEFAULT_ARGS = {
    'owner': 'CF_team',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 15),
    'email': ['atrukhova@ozon.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    #'pool': 'CF_team'
}
_PIP_OPTIONS = [
    
    "--trusted-host", "pypi.org",
    
]

def get_connection_as_json(conn_name):
    conn = BaseHook.get_connection(conn_name)
    conn_info = {
        'host': conn.host,
        'password': conn.password,
        'login': conn.login,
        'port': conn.port,
        'db': conn.schema
    }
    return json.dumps(conn_info)


dag = DAG('CF_factoring',
         default_args=DEFAULT_ARGS,
         schedule_interval="30 2 * * *",
         catchup=False,
         user_defined_macros={'get_connection_as_json': get_connection_as_json})   

def run_dag_factoring(*args, **kwargs):
    from airflow.models import Variable
    import json
    from at_factoring.main import factoring

    connection_vertica = json.loads(kwargs['connection_vertica'])
    connection_ms = json.loads(kwargs['connection_ms'])

    conn_info_vertica = {
        'host': str(connection_vertica['host']),
        'port': str(connection_vertica['port']),
        'user': str(connection_vertica['login']),
        'password': str(connection_vertica['password']),
        'database': str(connection_vertica['db'])    }

    conn_info_ms  = {
    'host': str(connection_ms['host']),
    'user': str(connection_ms['login']),
    'password': str(connection_ms['password'])}

    factoring(conn_info_vertica, conn_info_ms)

def run_dag_factoring_future(*args, **kwargs):
    from airflow.models import Variable
    import json
    from at_factoring.main_future import factoring_future

    connection_vertica = json.loads(kwargs['connection_vertica'])
    connection_ms = json.loads(kwargs['connection_ms'])

    conn_info_vertica = {
        'host': str(connection_vertica['host']),
        'port': str(connection_vertica['port']),
        'user': str(connection_vertica['login']),
        'password': str(connection_vertica['password']),
        'database': str(connection_vertica['db'])    }

    conn_info_ms  = {
    'host': str(connection_ms['host']),
    'user': str(connection_ms['login']),
    'password': str(connection_ms['password'])}

    factoring_future(conn_info_vertica, conn_info_ms)

t1 = PythonVirtualEnvOperatorCustomPip(
        task_id='factoring',
        requirements=['at_factoring','apache-airflow==1.10.10', 'cryptography==3.3.2'],
        op_kwargs={'connection_vertica': " {{ get_connection_as_json('CF_team_vertica') }} ",
                   'connection_ms': " {{ get_connection_as_json('CF_team_ms')}}", },

       pip_params_ls=['--trusted-host', 'artifactory.s.o3.ru', '--index-url', 'https://artifactory.s.o3.ru/artifactory/api/pypi/pypi-virtual/simple'], 
        python_version='3',
        queue='apps04',
        use_dill=False,
        python_callable=run_dag_factoring,
        dag=dag)

t2 = PythonVirtualEnvOperatorCustomPip(
        task_id='factoring_future',
        requirements=['at_factoring','apache-airflow==1.10.10', 'cryptography==3.3.2'],
        op_kwargs={'connection_vertica': " {{ get_connection_as_json('CF_team_vertica') }} ",
                   'connection_ms': " {{ get_connection_as_json('CF_team_ms')}}", },

       pip_params_ls=['--trusted-host', 'artifactory.s.o3.ru', '--index-url', 'https://artifactory.s.o3.ru/artifactory/api/pypi/pypi-virtual/simple'], 
        python_version='3',
        queue='apps04',
        use_dill=False,
        python_callable=run_dag_factoring_future,
        dag=dag)

t1 >> t2

