import logging
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import register_adapter, AsIs


class DataManager:
    def __init__(self, conn, engine, dict_all_data, data_to_closed):
        self.conn = conn
        self.cur = conn.cursor()
        self.schema = 'inside_core_schema'
        self.front_schema = 'core_schema'
        self.engine = engine
        self.data_to_closed = data_to_closed
        self.data = dict_all_data
        self.dictionary_tables_lst = ['raw_job_formats', 'raw_job_types', 'raw_languages',
                                      'raw_companies', 'raw_sources', 'raw_specialities',
                                      'raw_skills', 'raw_towns']
        self.link_tables_lst = ['job_formats_vacancies', 'job_types_vacancies', 'languages_vacancies',
                                'specialities_vacancies', 'skills_vacancies', 'towns_vacancies']
        """
        Необходимо добавить все наборы данных от DS
        """

    # def new_update_foo(self):
    #     logging.info("Loading data to vacancies")
    #     if not self.data.get('vacancies').empty:
    #         try:
    #             self.load_data_to_dicts()
    #             ids_to_update = tuple(self.data.get('vacancies')['id'].tolist())
    #             for link_table in self.link_tables_lst:
    #                 delete_query = f"""
    #                 DELETE FROM {self.schema}.{link_table} WHERE vacancy_id IN %s
    #                 """
    #                 self.cur.execute(delete_query, (ids_to_update,))
    #             self.load_data_to_vacancies()
    #             self.load_data_to_links()
    #             self.update_tech_table()
    #             self.conn.commit()
    #             self.conn.close()
    #         except Exception as e:
    #             logging.error(f"Error: {e}")
    #             self.conn.rollback()

    # Type fixing
    def fix_type(self):
        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        register_adapter(np.int64, addapt_numpy_int64)
        register_adapter(np.float64, addapt_numpy_float64)

    # Load data to DS dictionaries
    def load_without_changes(self, df_name):
        df = self.data[df_name]
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
            logging.info(f'No data to loading, dataframe {df_name} is empty')

    # Load data to core dictionaries 
    def load_dictionaries_core(self, df_name):
        df = self.data[df_name]
        if not df.empty:

            self.fix_type()

            selected_columns = df[['id', 'title']].copy()
            data_to_load = [tuple(x) for x in selected_columns.to_records(index=False)]

            # selected_columns.to_sql(str(df_name).replace('raw_', ''), self.engine,
            #                         schema=self.schema, if_exists='append', index=False)

            cols = ', '.join(list(selected_columns))
            update_query = f"""
            INSERT INTO {self.schema}.{df_name} 
            VALUES ({', '.join(['%s'] * len(list(selected_columns)))})
            ON CONFLICT (id) DO UPDATE
            SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in list(selected_columns)])})
            """
            self.cur.executemany(update_query, data_to_load)
        else:
            logging.info(f'No data to loading, dataframe {df_name} is empty')

    # Load data to all dictionaries (union, no commit)
    def load_data_to_dicts(self):
        self.fix_type()
        logging.info('Loading dictionary tables data')
        for dict_table_name in self.dictionary_tables_lst:
            try:
                # Load DS tables
                self.load_without_changes(dict_table_name)
                # Load core tables
                self.load_dictionaries_core(dict_table_name)
                logging.info(f'Data loaded successfully to {dict_table_name}')
                self.conn.commit()
            except Exception as e:
                logging.error(f"Error while data loading to dictionary {dict_table_name}: {e}")
                self.conn.rollback()

    # Init vacancies loading (union, no commit)
    def load_data_to_vacancies(self):
        logging.info('Loading data to vacancies')
        if not self.data.get('vacancies').empty:
            try:
                logging.info("Loading actual data into vacancies")
                self.data.get('vacancies').to_sql('vacancies', self.engine, schema=self.schema, if_exists='append',
                                                  index=False)
                logging.info("Loading actual vectors")
                self.data.get('ds_search').to_sql('ds_search', self.engine, schema=self.schema, if_exists='append',
                                                  index=False)
                logging.info("Completed")
                self.conn.commit()
            except Exception as e:
                logging.error(f"Error while data loading to vacancies: {e}")
                self.conn.rollback()

    # Init loading and updating links tables    
    def load_data_to_links(self):
        logging.info('Loading data to links tables')
        for link_table_name in self.link_tables_lst:
            df = self.data[link_table_name]
            if not df.empty:
                self.fix_type()
                logging.info("Deleteing old links")
                # Delete updating vacancies
                delete_updating_data_query = f"""
                DELETE FROM {self.schema}.{link_table_name} WHERE vacancy_id IN %s
                """
                self.cur.execute(delete_updating_data_query, (tuple(df['vacancy_id'].tolist()),))
                # Loading data to core
                logging.info("Loading data")
                data_to_load = [tuple(x) for x in df.to_records(index=False)]
                load_data_query = f"""
                INSERT INTO {self.schema}.{link_table_name}
                VALUES ({', '.join(['%s'] * len(list(df)))})
                """
                self.cur.executemany(load_data_query, data_to_load)
                logging.info('Completed')
            else:
                logging.info(f'No data to update {link_table_name}')

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
        SELECT MAX(id) AS max_id FROM {self.schema}.archive_vacancies)
        INSERT INTO {self.schema}.vacancies_max_id
        (SELECT MAX(max_id)
        FROM union_table)
        """
        self.cur.execute(find_max_query)

    # Actualize core data (archiving), excluding dictionaries (union, commit)
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
            WHERE table_schema = '{self.schema}' AND table_name = 'archive_vacancies'
            ORDER BY ordinal_position;
            """
            self.cur.execute(select_vacancy_columns)
            na_vacancies_cols = [row[0] for row in self.cur.fetchall()]

            # Upsert archive vacancies table
            cols = ','.join(na_vacancies_cols)
            load_not_actual_data_to_na_vacancy = f"""
            INSERT INTO {self.schema}.archive_vacancies
            SELECT * FROM {self.schema}.vacancies
            WHERE url IN %s
            ON CONFLICT (id) DO UPDATE
            SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in na_vacancies_cols])})
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
                logging.info("Archive tables updated successfully")
            except Exception as e:
                logging.error(f"Error: {e}")
                self.conn.rollback()
        else:
            logging.info("No data to remove to archive")

    # Process. Update data on core-layer (union, commit)
    def load_and_update_actual_data(self):
        if not self.data.get('vacancies').empty:
            try:
                self.fix_type()

                # Loading to dictionaries
                self.load_data_to_dicts()
                # self.vacancies = self.vacancies.where(pd.notna(self.vacancies), 'nan')
                logging.info("New data loading to vacancies")

                # Datatype fixing (NaN -> NULL)
                vacancies = self.data.get('vacancies').fillna(psycopg2.extensions.AsIs('NULL'))

                # Loading data to vacancies
                data_to_load = [tuple(x) for x in vacancies.to_records(index=False)]
                names = list(vacancies)
                cols = ', '.join(names)
                vacancies_update_query = f"""
                INSERT INTO {self.schema}.vacancies 
                VALUES ({', '.join(['%s'] * len(list(vacancies)))})
                ON CONFLICT (id) DO UPDATE
                SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in names])});
                """
                self.cur.executemany(vacancies_update_query, data_to_load)

                logging.info("New data loading to search tables")

                # Load data to ds_search
                data_to_load = [tuple(x) for x in self.data.get('ds_search').to_records(index=False)]
                names = list(self.data.get('ds_search'))
                cols = ', '.join(names)
                ds_search_update = f"""
                INSERT INTO {self.schema}.ds_search
                VALUES ({', '.join(['%s'] * len(names))})
                ON CONFLICT (vacancy_id) DO UPDATE
                SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in names])});
                """
                self.cur.executemany(ds_search_update, data_to_load)

                # Load data to links
                logging.info("Loading data to links")
                self.load_data_to_links()

                # Update max id
                self.update_tech_table()
                self.conn.commit()
            except Exception as e:
                logging.error(f'Error while loading data to core tables: {e}')
                self.conn.rollback()
        else:
            logging.info("No data to update")

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
            logging.info(f"Error while init data loading to core: {e}")

