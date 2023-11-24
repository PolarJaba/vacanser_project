import json
import psycopg2
from airflow import settings
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
from airflow import settings
from sqlalchemy import create_engine
from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import logging
from logging import handlers
from airflow import models
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine


# Connections settings
# Загружаем данные подключений из JSON файла
with open('/opt/airflow/dags/config_connections.json', 'r') as conn_file:
    connections_config = json.load(conn_file)

# Получаем данные конфигурации подключения и создаем конфиг для клиента
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
engine = create_engine(f"postgresql+psycopg2://{conn_config['user']}:{conn_config['password']}@{conn_config['host']}:{conn_config['port']}/{conn_config['database']}")


# Variables settings
# Загружаем переменные из JSON файла
with open('/opt/airflow/dags/config_variables.json', 'r') as config_file:
    my_variables = json.load(config_file)

# Проверяем, существует ли переменная с данным ключом
if not Variable.get("shares_variable", default_var=None):
    # Если переменная не существует, устанавливаем ее
    Variable.set("shares_variable", my_variables, serialize_json=True)

dag_variables = Variable.get("shares_variable", deserialize_json=True)

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

# Параметры по умолчанию
default_args = {
    "owner": "admin_1T",
    # 'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=5),
}

raw_tables = ['raw_vk', 'raw_sber', 'raw_tinkoff', 'raw_yandex']

raw_job_formats = pd.read_csv(r'/opt/airflow/from_DS/raw_job_formats.csv')
raw_job_types = pd.read_csv(r'/opt/airflow/from_DS/raw_job_types.csv')
raw_languages = pd.read_csv(r'/opt/airflow/from_DS/raw_languages.csv')
raw_companies = pd.read_csv(r'/opt/airflow/from_DS/raw_companies.csv')
raw_sources = pd.read_csv(r'/opt/airflow/from_DS/raw_sources.csv')
raw_specialities = pd.read_csv(r'/opt/airflow/from_DS/raw_specialities.csv')
raw_skills = pd.read_csv(r'/opt/airflow/from_DS/raw_skills.csv')
raw_towns = pd.read_csv(r'/opt/airflow/from_DS/raw_towns.csv')

job_formats_vacancies = pd.read_csv(r'/opt/airflow/from_DS/job_formats_vacancies.csv')
job_types_vacancies = pd.read_csv(r'/opt/airflow/from_DS/job_types_vacancies.csv')
languages_vacancies = pd.read_csv(r'/opt/airflow/from_DS/languages_vacancies.csv')
specialities_vacancies = pd.read_csv(r'/opt/airflow/from_DS/specialities_vacancies.csv')
skills_vacancies = pd.read_csv(r'/opt/airflow/from_DS/skills_vacancies.csv')
towns_vacancies = pd.read_csv(r'/opt/airflow/from_DS/towns_vacancies.csv')

ds_search = pd.read_csv(r'/opt/airflow/from_DS/ds_search.csv')


class DatabaseManager:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.log = LoggingMixin().log
        self.schema = 'core_schema'

    def create_tech_table(self):
        try:
            tech_schema_query = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.vacancies_max_id(
            max_id INT
            );
            """
            self.cur.execute(tech_schema_query)
            self.log.info("Создание контрольной таблицы успешно завершено")
        except Exception as e:
            self.log.error(f"При создании контрольной таблицы произошла ошибка: {e}")
    def create_raw_dictionaries(self):
        try:
            # create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {self.schema}"
            # self.cur.execute(create_schema_query)
            # self.log.info(f'Создана схема {self.schema}')

            create_raw_dict_table_query = f"""                
            CREATE TABLE IF NOT EXISTS {self.schema}.raw_job_formats(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_languages(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_skills(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50), 
            clear_title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_job_types(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_specialities(
            id SERIAL PRIMARY KEY,
            title VARCHAR(100), 
            tag VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_towns(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50), 
            clear_title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_sources(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_companies(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );
            """
            self.cur.execute(create_raw_dict_table_query)
            self.conn.commit()
            self.log.info(f'Таблицы словарей слоя сырых данных успешно создвны, запрос: "create_raw_dict_table_query"')
        except Exception as e:
            self.log.error(f'Ошибка во время выполнения запроса "create_raw_dict_table_query": {e}')
            self.conn.rollback()   


    def create_dictionaries(self):    
        try:
            create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {self.schema}"
            self.cur.execute(create_schema_query)
            self.log.info('Схема создана') 
            self.conn.commit()  
        except Exception as e:
            self.log.error('Неудачная попытка создания схемы')
        
        try:
            create_dict_table_query = f"""                
            CREATE TABLE IF NOT EXISTS {self.schema}.job_formats(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.languages(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.skills(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.job_types(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.specialities(
            id SERIAL PRIMARY KEY,
            title VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.towns(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.sources(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.companies(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );
            """
            self.cur.execute(create_dict_table_query)
            self.conn.commit()
            self.log.info(f'Таблицы словарей успешно созданы, запрос: "create_dict_table_query"')
        except Exception as e:
            self.log.error(f'Ошибка при выполнении запроса "create_dict_table_query": {e}')
            self.conn.rollback()


    def create_vacancies_table(self):
        try:
            create_vacancy_table_query = f""" 
            CREATE TABLE IF NOT EXISTS {self.schema}.vacancies(
            id BIGINT PRIMARY KEY,
            "version" INT NOT NULL,
            "url" VARCHAR(2073) NOT NULL,
            title VARCHAR(255),
            salary_from DECIMAL(10,2),
            salary_to DECIMAL(10,2),
            experience_from DECIMAL(2,1),
            experience_to DECIMAL(2,1),
            "description" TEXT,
            company_id INT,
            FOREIGN KEY (company_id) REFERENCES {self.schema}.companies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            source_id INT,
            FOREIGN KEY (source_id) REFERENCES {self.schema}.sources (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            publicated_at DATE
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.ds_search(
            vacancy_id BIGINT PRIMARY KEY,
            vector TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );           
            """   
            self.cur.execute(create_vacancy_table_query)
            self.conn.commit()
            self.log.info(f'Tables vacancies and ds_search created successfully, query: "create_vacancy_table_query"')
        except Exception as e:
            self.log.error(f'Ошибка при выполнении запроса "create_vacancy_table_query": {e}')
            self.conn.rollback()


    def create_not_actual_vacancies_table(self):
        try:
            create_not_actual_vacancy_table_query = f""" 
            CREATE TABLE IF NOT EXISTS {self.schema}.not_actual_vacancies(
            id BIGINT PRIMARY KEY,
            "version" INT NOT NULL,
            "url" VARCHAR(2073) NOT NULL,
            title VARCHAR(255),
            salary_from DECIMAL(10,2),
            salary_to DECIMAL(10,2),
            experience_from DECIMAL(2,1),
            experience_to DECIMAL(2,1),
            "description" TEXT,
            company_id INT,
            FOREIGN KEY (company_id) REFERENCES {self.schema}.raw_companies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            source_id INT,
            FOREIGN KEY (source_id) REFERENCES {self.schema}.raw_sources (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            publicated_at DATE
            );
            """   
            self.cur.execute(create_not_actual_vacancy_table_query)
            self.conn.commit()
            self.log.info(f'Таблица vacancies успешно создана, запрос: "create_not_actual_vacancy_table_query"')
        except Exception as e:
            self.log.error(f'Ошибка при выполнении запроса "create_deleted_not_actual_table_query": {e}')
            self.conn.rollback()

    def create_not_actual_link_tables(self):
        try:
            create_not_actual_link_tables_query = f""" 
            CREATE TABLE IF NOT EXISTS {self.schema}.not_actual_job_formats_vacancies (
            vacancy_id BIGINT,
            job_format_id INT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.not_actual_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (job_format_id) REFERENCES {self.schema}.raw_job_formats (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.not_actual_languages_vacancies (
            vacancy_id BIGINT,
            language_id INT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.not_actual_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (language_id) REFERENCES {self.schema}.raw_languages (id) ON UPDATE CASCADE ON DELETE RESTRICT	
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.not_actual_skills_vacancies (
            vacancy_id BIGINT,
            skill_id INT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.not_actual_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (skill_id) REFERENCES {self.schema}.raw_skills (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.not_actual_job_types_vacancies (
            vacancy_id BIGINT,
            job_type_id INT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.not_actual_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (job_type_id) REFERENCES {self.schema}.raw_job_types (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.not_actual_specialities_vacancies (
            vacancy_id BIGINT,
            spec_id INT,
            concurrence_percent DECIMAL(4,1),
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.not_actual_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (spec_id) REFERENCES {self.schema}.raw_specialities (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.not_actual_towns_vacancies (
            vacancy_id BIGINT,
            town_id INT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.not_actual_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (town_id) REFERENCES {self.schema}.raw_towns (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );
            """   
            self.cur.execute(create_not_actual_link_tables_query)
            self.conn.commit()
            self.log.info(f'Таблицы связей успешно создана, запрос: "create_not_actual_link_tables_query"')
        except Exception as e:
            self.log.error(f'Ошибка при выполнении запроса "create_not_actual_link_tables_query": {e}')
            self.conn.rollback()


    def create_link_tables(self):
        try:
            create_link_tables_query = f""" 
            CREATE TABLE IF NOT EXISTS {self.schema}.job_formats_vacancies (
            vacancy_id BIGINT,
            job_format_id INT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (job_format_id) REFERENCES {self.schema}.job_formats (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.languages_vacancies (
            vacancy_id BIGINT,
            language_id INT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (language_id) REFERENCES {self.schema}.languages (id) ON UPDATE CASCADE ON DELETE RESTRICT	
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.skills_vacancies (
            vacancy_id BIGINT,
            skill_id INT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (skill_id) REFERENCES {self.schema}.skills (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.job_types_vacancies (
            vacancy_id BIGINT,
            job_type_id INT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (job_type_id) REFERENCES {self.schema}.job_types (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.specialities_vacancies (
            vacancy_id BIGINT,
            spec_id INT,
            concurrence_percent DECIMAL(4,1),
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (spec_id) REFERENCES {self.schema}.specialities (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.towns_vacancies (
            vacancy_id BIGINT,
            town_id INT,
            FOREIGN KEY (vacancy_id) REFERENCES {self.schema}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (town_id) REFERENCES {self.schema}.towns (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );
            
            CREATE TABLE IF NOT EXISTS {self.schema}.ds_search(
            id BIGINT,
            vector TEXT,
            FOREIGN KEY (id) REFERENCES {self.schema}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT
            );
            """   
            self.cur.execute(create_link_tables_query)
            self.conn.commit()
            self.log.info(f'Таблицы связей успешно создана, запрос: "create_link_tables_query"')
        except Exception as e:
            self.log.error(f'Ошибка при выполнении запроса "create_link_tables_query": {e}')
            self.conn.rollback()


class DataManager:
    def __init__(self, conn, engine):
        self.conn = conn
        self.cur = conn.cursor()
        self.log = LoggingMixin().log
        self.schema = 'core_schema'
        self.engine = engine
        # Обновление списков таблиц можно прописать в связке с получением данных меты
        # Лучше связать с config
        self.dictionary_tables_lst = ['raw_job_formats', 'raw_job_types', 'raw_languages',
                                      'raw_companies', 'raw_sources', 'raw_specialities',
                                      'raw_skills', 'raw_towns']
        self.link_tables_lst = ['job_formats_vacancies', 'job_types_vacancies', 'languages_vacancies',
                                 'specialities_vacancies', 'skills_vacancies', 'towns_vacancies']
        """
        Необходимо добавить все наборы данных от DS
        """
        self.vacancies = pd.read_csv(r'/opt/airflow/from_DS/vacancies.csv')
         
       
    def new_update_foo(self):
        self.log.info("Начинается загрузка")
        if not self.vacancies.empty:
            try:
                self.load_data_to_dicts()
                ids_to_update = tuple(self.vacancies['id'].tolist())
                for link_table in self.link_tables_lst:
                    delete_query = f"""
                    DELETE FROM {self.schema}.{link_table} WHERE vacancy_id IN %s
                    """
                    self.cur.execute(delete_query, (ids_to_update,))
                self.load_data_to_vacancies()
                self.load_data_to_links()
                self.update_tech_table()
                self.conn.commit()
                self.conn.close()
            except Exception as e:
                self.log.error(f"Ошибка при обновлении данных: {e}")
                self.conn.rollback()

    # Type fixing
    def fix_type(self):
        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)
        
        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)
            
        register_adapter(np.int64, addapt_numpy_int64)
        register_adapter(np.float64, addapt_numpy_float64)  
        

    # Load data to DS (archive) dictionaries                 
    def load_without_changes(self, df_name):
        df = globals()[df_name]
        if not df.empty:
            
            self.fix_type()
            
            # df.to_sql(df_name, self.engine, schema=self.schema, if_exists='append', index=False)
            data_to_load = [tuple(x) for x in df.to_records(index=False)]
            cols = ', '.join(list(df))
            update_query = f"""
            INSERT INTO {self.schema}.{df_name} 
            VALUES ({', '.join(['%s'] * len(list(df)))})
            ON CONFLICT (id) DO UPDATE
            SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in list(df)])})
            """
            self.cur.executemany(update_query, data_to_load)
        else:
            self.log.info(f'No data to loading, dataframe {df_name} is empty')


    # Load data to core dictionaries 
    def load_dictionaries_core(self, df_name):
        df = globals()[df_name]
        if not df.empty:
        
            self.fix_type()
            
            selected_columns = df[['id', 'title']].copy()
            data_to_load = [tuple(x) for x in selected_columns.to_records(index=False)]

            # selected_columns.to_sql(str(df_name).replace('raw_', ''), self.engine, schema=self.schema, if_exists='append', index=False)
            
            table_name = str(f'{df_name}').replace('raw_', '')
            cols = ', '.join(list(selected_columns))
            update_query = f"""
            INSERT INTO {self.schema}.{table_name} 
            VALUES ({', '.join(['%s'] * len(list(selected_columns)))})
            ON CONFLICT (id) DO UPDATE
            SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in list(selected_columns)])})
            """
            self.cur.executemany(update_query, data_to_load)
        else:
            self.log.info(f'No data to loading, dataframe {df_name} is empty')


    # Load data to all dictionaries (union, no commit)
    def load_data_to_dicts(self):
        self.fix_type()
        self.log.info('Loading dictionary tables data')
        for dict_table_name in self.dictionary_tables_lst:
            try:
                # Load DS tables
                self.load_without_changes(dict_table_name)
                # Load core tables
                self.load_dictionaries_core(dict_table_name)
                self.log.info(f'Data loaded successfully to {dict_table_name}')
                self.conn.commit()
            except Exception as e:
                self.log.error(f"Error while data loading to dictionary {dict_table_name}: {e}")
                self.conn.rollback()


    # Init vacancies loading (union, no commit)
    def load_data_to_vacancies(self):
        self.log.info('Loading data to vacancies')
        if not self.vacancies.empty:
            # self.log.info("Deleting old data")
            # ids_tuple = tuple(self.vacancies['id'].tolist())
            # delete_old_data = f"""
            # DELETE FROM {self.schema}.vacancies WHERE id IN %s
            # """
            # self.cur.execute(delete_old_data, (ids_tuple,))
            # self.log.info("Loading actual data into vacancies")
            try:
                self.vacancies.to_sql('vacancies', self.engine, schema=self.schema, if_exists='append', index=False)
                self.log.info("Loading actual vectors")
                self.conn.commit()
            except Exception as e:
                self.log.error(f"Error while data loading to vacancies: {e}")
                self.conn.rollback()
            else:
                try:
                    ds_search.to_sql('ds_search', self.engine, schema=self.schema, if_exists='append', index=False)
                    self.log.info("Completed")
                    self.conn.commit()
                except Exception as e:
                    self.log.error(f"Error while data loading to ds table: {e}")
                    self.conn.rollback()



    # Init loading and updating links tables    
    def load_data_to_links(self):
        self.log.info('Loading data to links tables')
        for link_table_name in self.link_tables_lst:
            df = globals()[link_table_name]
            if not df.empty: 
                self.fix_type()
                self.log.info("Deleteing old links")
                # Удаление данных для обновляемых вакансий
                delete_updating_data_query = f"""
                DELETE FROM {self.schema}.{link_table_name} WHERE vacancy_id IN %s
                """
                self.cur.execute(delete_updating_data_query, (tuple(df['vacancy_id'].tolist()),))
                # Загрузка данных на core
                self.log.info("Loading data")
                data_to_load = [tuple(x) for x in df.to_records(index=False)]
                load_data_query = f"""
                INSERT INTO {self.schema}.{link_table_name}
                VALUES ({', '.join(['%s'] * len(list(df)))})
                """
                self.cur.executemany(load_data_query, data_to_load)
                self.log.info('Completed') 
            else:
                self.log.info(f'No data to update {link_table_name}')


    # Max ID update
    def update_tech_table(self):
        clearing_query = f"""
        TRUNCATE TABLE {self.schema}.vacancies_max_id
        """
        self.cur.execute(clearing_query)
        find_max_query = f"""
        WITH union_table AS (
        SELECT MAX(id) AS max_id FROM {self.schema}.vacancies
        UNION ALL
        SELECT MAX(id) AS max_id FROM {self.schema}.not_actual_vacancies)
        INSERT INTO {self.schema}.vacancies_max_id
        (SELECT MAX(max_id)
        FROM union_table)
        """
        self.cur.execute(find_max_query)


    # Actualize core data (archivate), excluding dictionaries (union, commit)
    def delete_not_actual_core_data(self):

        self.fix_type()

        # Select actual data
        core_data_load_query = f"""
        SELECT id, url  FROM {self.schema}.vacancies
        """
        core_data = pd.read_sql(core_data_load_query, self.engine)
        # Для оптимизации работы переопределять raw_tables
        # for table in raw_tables:
            # raw_actual = f"""
            # SELECT vacancy_id AS url, MAX(version_vac) AS version 
            # FROM {self.schema}.{table}
            # WHERE status != 'closed' 
            # GROUP BY vacancy_id
            # """
            # raw_actual_data = raw_actual_data.append(pd.read_sql(raw_actual, self.engine), ignore_index=True)

        # Получение списка неактуальных вакансий на core 
        # not_actual_df = core_data[~core_data['url'].isin(raw_actual_data['url'])]            
        # not_actual_urls = set(core_data['url']).difference(set(raw_data['url']))

        """
        Предполагается получение датафрейма неактуальных вакансий
        при запросе аналогичных данных для обновления raw
        ЗАМЕНИТЬ НА РЕАЛЬНЫЙ df
        """
        not_actual_df = pd.DataFrame({'url': ['https://rabota.sber.ru/search/4219605',
                                              'https://rabota.sber.ru/search/4221748'],
                                      'version': [1, 1],
                                      'id': [1, 2]})
        if not not_actual_df.empty:
            
            # Load vacancies' columns list 
            select_vacancy_columns = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{self.schema}' AND table_name = 'not_actual_vacancies'
            ORDER BY ordinal_position;
            """
            self.cur.execute(select_vacancy_columns)
            na_vacansies_cols = [row[0] for row in self.cur.fetchall()]

            # Upsert archive vacancies table
            cols = ','.join(na_vacansies_cols)
            load_not_actual_data_to_na_vacancy = f"""
            INSERT INTO {self.schema}.not_actual_vacancies
            SELECT * FROM {self.schema}.vacancies
            WHERE url IN %s
            ON CONFLICT (id) DO UPDATE
            SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in na_vacansies_cols])})
            """     

            urls_tuple = tuple(not_actual_df['url'].tolist())
            ids_tuple = tuple(not_actual_df['id'].tolist())
            # Load data to archive vacancies table
            try:
                # Upsert archive vacancies table
                self.cur.execute(load_not_actual_data_to_na_vacancy, (urls_tuple,))

                # Update links tables
                for table_name in self.link_tables_lst:

                    # Delete old links
                    delete_updating_data_query = f"""
                    DELETE FROM {self.schema}.not_actual_{table_name} WHERE vacancy_id IN %s
                    """
                    self.cur.execute(delete_updating_data_query, (ids_tuple,))

                    # Remove old links
                    move_data_query = f"""
                    INSERT INTO {self.schema}.not_actual_{table_name} 
                    SELECT * FROM {self.schema}.{table_name}
                    WHERE vacancy_id IN %s
                    """

                    # Delete closed vacancies from core
                    delete_data_query = f"""
                    DELETE FROM {self.schema}.{table_name} WHERE vacancy_id IN %s
                    """
                    self.cur.execute(move_data_query, (ids_tuple,))
                    self.cur.execute(delete_data_query, (ids_tuple,))

                # Delete not actual vacancies from core    
                delete_not_actual_data_ds = f"""
                DELETE FROM {self.schema}.ds_search WHERE vacancy_id IN %s;
                """
                delete_not_actual_data_vacancies = f"""
                DELETE FROM {self.schema}.vacancies WHERE url IN %s;
                """
                self.cur.execute(delete_not_actual_data_ds, (ids_tuple,))
                self.cur.execute(delete_not_actual_data_vacancies, (urls_tuple,))
                self.conn.commit()
                self.log.info("Archive tables updated successfully")
            except Exception as e:
                self.log.error(f"Error: {e}")
                self.conn.rollback()    
        else:
            self.log.info("No data to remove to archive")

    # Process. Update data on core-layer (union, commit)
    def load_and_update_actual_data(self):   
        if not self.vacancies.empty:
            try:
                self.fix_type()

                # Loading to dictionaries
                self.load_data_to_dicts()
                # self.vacancies = self.vacancies.where(pd.notna(self.vacancies), 'nan')
                self.log.info("New data loading to vacancies")

                # Datatype fixing (NaN -> NULL)
                self.vacancies = self.vacancies.fillna(psycopg2.extensions.AsIs('NULL'))
                
                # Loading data to vacancies
                data_to_load = [tuple(x) for x in self.vacancies.to_records(index=False)]
                names = list(self.vacancies)
                cols = ', '.join(names)
                vacancies_update_query = f"""
                INSERT INTO {self.schema}.vacancies 
                VALUES ({', '.join(['%s'] * len(list(self.vacancies)))})
                ON CONFLICT (id) DO UPDATE
                SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in names])});
                """
                self.cur.executemany(vacancies_update_query, data_to_load)
                
                self.log.info("New data loading to search tables")

                # Load data to ds_search
                data_to_load = [tuple(x) for x in ds_search.to_records(index=False)]
                names = list(ds_search)
                cols = ', '.join(names)
                ds_search_update = f"""
                INSERT INTO {self.schema}.ds_search
                VALUES ({', '.join(['%s'] * len(list(ds_search)))})
                ON CONFLICT (vacancy_id) DO UPDATE
                SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in names])});
                """
                self.cur.executemany(ds_search_update, data_to_load)

                # Load data to links
                self.log.info("Loading data to links")
                self.load_data_to_links()

                # Update max id
                self.update_tech_table()
                self.conn.commit()
            except Exception as e:
                self.log.error(f'Error while loading data to core tables: {e}')
                self.conn.rollback()
        else:
            self.log.info("No data to update")
        
    # Process. Init data loading (union, commit)
    def init_load(self):
        try:
            self.load_data_to_dicts()
            self.load_data_to_vacancies()
            self.load_data_to_links()
            self.update_tech_table()
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            self.log.info(f"Error while init data loading to core: {e}")
    




"""
Последовательности команд для DAG'ов
"""
def create_core_layer(**context):
    """
    Создание таблиц core-слоя
    + таблицы удаленных вакансий
    """
    log = context['ti'].log
    log.info('Core, archive and DS tables creating ')
    try:
        worker = DatabaseManager(conn)
        worker.create_dictionaries()
        worker.create_raw_dictionaries()
        worker.create_vacancies_table()
        worker.create_tech_table()
        worker.create_not_actual_vacancies_table()
        worker.create_link_tables()
        worker.create_not_actual_link_tables()
        log.info('Completed')
    except Exception as e:
        log.error(f'Error: {e}')

     
def init_load_data(**context):
    """
    Первичная загрузка данных в core
    + таблицы удаленных вакансий
    """
    log = context['ti'].log
    log.info('Initializing data loading to core')
    try:
        worker = DataManager(conn, engine)
        worker.init_load()
        log.info('Loading completed')
    except Exception as e:
        log.error(f'Error while loading data to core layer: {e}')


def delete_not_actual_data(**context):
    """
    Перенос неактуальных данных в таблицы удаленных вакансий
    Удалние из слоя core
    """
    log = context['ti'].log
    log.info('Actualize core data, upsert archive tables')
    try:
        worker = DataManager(conn, engine)
        worker.delete_not_actual_core_data()
        log.info('Completed')
    except Exception as e:
        log.error(f'Error while actualizing core data: {e}')


def update_and_delta_loading(**context):
    """
    Обновление и загрузка дельты данных
    (Загрузка результата от модели)
    """
    log = context['ti'].log
    log.info('Update data on core')
    try:
        worker = DataManager(conn, engine)
        worker.load_and_update_actual_data()
        log.info('Completed')
    except Exception as e:
        log.error(f'Error while updating core data: {e}')


# DAG ручного запуска (ddl)
initial_dag = DAG(dag_id='golden_core_initial_dag',
                tags=['admin_1T'],
                start_date=datetime(2023, 11, 11),
                schedule_interval=None,
                default_args=default_args
                )

# DAG'и ручного запуска (dml)
init_load_dag = DAG(dag_id='init_load_to_golden_core',
                tags=['admin_1T'],
                start_date=datetime(2023, 11, 11),
                schedule_interval=None,
                default_args=default_args
                )

archive_dag = DAG(dag_id='work_with_archive',
                tags=['admin_1T'],
                start_date=datetime(2023, 11, 11),
                schedule_interval=None,
                default_args=default_args
                )

update_dag = DAG(dag_id='update_golden_core',
                tags=['admin_1T'],
                start_date=datetime(2023, 11, 11),
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

# Определение задачи
create_golden_core = PythonOperator(
    task_id='create_raw_tables',
    python_callable=create_core_layer,
    provide_context=True,
    dag=initial_dag
)

init_golden_core = PythonOperator(
    task_id='init_load_data',
    python_callable=init_load_data,
    provide_context=True,
    dag=init_load_dag
)

move_data_from_golden_core = PythonOperator(
    task_id='move_data_to_archive',
    python_callable=delete_not_actual_data,
    provide_context=True,
    dag=archive_dag
)

update_golden_core = PythonOperator(
    task_id='update_data_on_golden_core',
    python_callable=update_and_delta_loading,
    provide_context=True,
    dag=update_dag
)

hello_bash_task >> create_golden_core >> end_task
init_golden_core
move_data_from_golden_core
update_golden_core 


               
