

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
            title VARCHAR(50), 
            clear_title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_languages(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50), 
            clear_title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_skills(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50), 
            clear_title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_job_types(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50), 
            clear_title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_specialities(
            id SERIAL PRIMARY KEY,
            title VARCHAR(100), 
            clear_title VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_towns(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50), 
            clear_title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_sources(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50), 
            clear_title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {self.schema}.raw_companies(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50), 
            clear_title VARCHAR(50)
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
            id BIGSERIAL PRIMARY KEY,
            "version" INT NOT NULL,
            "url" VARCHAR(2073) NOT NULL,
            title VARCHAR(255),
            salary_from INT,
            salary_to INT,
            experience_from SMALLINT,
            experience_to SMALLINT,
            "description" TEXT,
            company_id INT,
            FOREIGN KEY (company_id) REFERENCES {self.schema}.companies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            source_id INT,
            FOREIGN KEY (source_id) REFERENCES {self.schema}.sources (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            vector DECIMAL,
            publicated_at DATE
            );
            """   
            self.cur.execute(create_vacancy_table_query)
            self.conn.commit()
            self.log.info(f'Таблица vacancies успешно создана, запрос: "create_vacancy_table_query"')
        except Exception as e:
            self.log.error(f'Ошибка при выполнении запроса "create_vacancy_table_query": {e}')
            self.conn.rollback()


    def create_not_actual_vacancies_table(self):
        try:
            create_not_actual_vacancy_table_query = f""" 
            CREATE TABLE IF NOT EXISTS {self.schema}.not_actual_vacancies(
            id BIGSERIAL PRIMARY KEY,
            "version" INT NOT NULL,
            "url" VARCHAR(2073) NOT NULL,
            title VARCHAR(255),
            salary_from INT,
            salary_to INT,
            experience_from SMALLINT,
            experience_to SMALLINT,
            "description" TEXT,
            company_id INT,
            FOREIGN KEY (company_id) REFERENCES {self.schema}.raw_companies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            source_id INT,
            FOREIGN KEY (source_id) REFERENCES {self.schema}.raw_sources (id) ON UPDATE CASCADE ON DELETE RESTRICT,
            vector DECIMAL,
            publicated_at DATE
            );
            """   
            self.cur.execute(create_not_actual_vacancy_table_query)
            self.conn.commit()
            self.log.info(f'Таблица vacancies успешно создана, запрос: "create_not_actual_vacancy_table_query"')
        except Exception as e:
            self.log.error(f'Ошибка при выполнении запроса "create_deleted_not_actual_table_query": {e}')
            self.conn.rollback()
