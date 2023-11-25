import json
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import logging
from logging import handlers
from airflow.models import Variable
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine
from ddl_core import DatabaseManager
from dml_core import DataManager
import pandas as pd


job_formats = pd.read_csv(r'/opt/airflow/from_DS/job_formats.csv')
job_types = pd.read_csv(r'/opt/airflow/from_DS/job_types.csv')
languages = pd.read_csv(r'/opt/airflow/from_DS/languages.csv')
companies = pd.read_csv(r'/opt/airflow/from_DS/companies.csv')
sources = pd.read_csv(r'/opt/airflow/from_DS/sources.csv')
specialities = pd.read_csv(r'/opt/airflow/from_DS/specialities.csv')
skills = pd.read_csv(r'/opt/airflow/from_DS/skills.csv')
towns = pd.read_csv(r'/opt/airflow/from_DS/towns.csv')
experience = pd.read_csv(r'/opt/airflow/from_DS/experience.csv')

job_formats_vacancies = pd.read_csv(r'/opt/airflow/from_DS/job_formats_vacancies.csv')
job_types_vacancies = pd.read_csv(r'/opt/airflow/from_DS/job_types_vacancies.csv')
languages_vacancies = pd.read_csv(r'/opt/airflow/from_DS/languages_vacancies.csv')
specialities_vacancies = pd.read_csv(r'/opt/airflow/from_DS/specialities_vacancies.csv')
skills_vacancies = pd.read_csv(r'/opt/airflow/from_DS/skills_vacancies.csv')
towns_vacancies = pd.read_csv(r'/opt/airflow/from_DS/towns_vacancies.csv')
experience_vacancies = pd.read_csv(r'/opt/airflow/from_DS/experience_vacancies.csv')
specialities_skills = pd.read_csv(r'/opt/airflow/from_DS/specialities_skills.csv')

ds_search = pd.read_csv(r'/opt/airflow/from_DS/ds_search.csv')
vacancies = pd.read_csv(r'/opt/airflow/from_DS/vacancies.csv')

dfs = {'job_formats': job_formats,
       'job_types': job_types,
       'languages': languages,
       'companies': companies,
       'sources': sources,
       'specialities': specialities,
       'skills': skills,
       'towns': towns,
       'experience': experience,
       'job_formats_vacancies': job_formats_vacancies,
       'job_types_vacancies': job_types_vacancies,
       'languages_vacancies': languages_vacancies,
       'specialities_vacancies': specialities_vacancies,
       'skills_vacancies': skills_vacancies,
       'towns_vacancies': towns_vacancies,
       'specialities_skills': specialities_skills,
       'experience_vacancies': experience_vacancies,
       'ds_search': ds_search,
       'vacancies': vacancies
       }

# Loading connections data from json
with open('/opt/airflow/dags/config_connections.json', 'r') as conn_file:
    connections_config = json.load(conn_file)

# Getting connection config data and client config creating
conn_config = connections_config['psql_connect']

config = {
    'database': conn_config['database'],
    'user': conn_config['user'],
    'password': conn_config['password'],
    'host': conn_config['host'],
    'port': conn_config['port'],
}

conn = psycopg2.connect(**config)
conn.autocommit = False
engine = create_engine(f"postgresql+psycopg2://{conn_config['user']}:{conn_config['password']}@{conn_config['host']}:"
                       f"{conn_config['port']}/{conn_config['database']}")

# Variables settings
# Loading variables from json
with open('/opt/airflow/dags/config_variables.json', 'r') as config_file:
    my_variables = json.load(config_file)

# Checking existence of variable
if not Variable.get("shares_variable", default_var=None):
    Variable.set("shares_variable", my_variables, serialize_json=True)

dag_variables = Variable.get("shares_variable", deserialize_json=True)

# Default dag arguments
default_args = {
    "owner": "admin_1T",
    # 'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=5),
}

# logging settings
logging_level = os.environ.get('LOGGING_LEVEL', 'DEBUG').upper()
logging.basicConfig(level=logging_level)
log = logging.getLogger(__name__)
log_handler = handlers.RotatingFileHandler('/opt/airflow/logs/airflow.log',
                                           maxBytes=5000,
                                           backupCount=5)

log_handler.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)
log.addHandler(log_handler)


def ddl_core():
    manager = DatabaseManager(conn)
    manager.db_creator()

def dml_core():
    manager = DataManager(conn, engine, dfs, pd.DataFrame({'vacancy_url': ['https://rabota.sber.ru/search/4219605',
                                                                           'https://rabota.sber.ru/search/4221748']}))
    manager.init_load()

ddl_dag = DAG(dag_id='core_ddl_dag',
              tags=['admin_1T'],
              start_date=datetime(2023, 11, 25),
              schedule_interval=None,
              default_args=default_args
              )

dml_init_dag = DAG(dag_id='core_dml_init_dag',
              tags=['admin_1T'],
              start_date=datetime(2023, 11, 25),
              schedule_interval=None,
              default_args=default_args
              )

hello_bash_task = BashOperator(
    task_id='hello_task',
    bash_command='echo "Удачи"'
)

end_task = DummyOperator(
    task_id="end_task"
)

create_golden_core = PythonOperator(
    task_id='create_core_tables',
    python_callable=ddl_core,
    provide_context=True,
    dag=ddl_dag
)

init_golden_core = PythonOperator(
    task_id='create_core_tables',
    python_callable=dml_core,
    provide_context=True,
    dag=dml_init_dag
)

hello_bash_task >> create_golden_core >> end_task
init_golden_core
