import pandas as pd
import numpy as np
import re
import spacy

from datetime import datetime
from pymystem3 import Mystem
from spacy.matcher import Matcher
from spacy.lang.en import English


from patterns_all import patterns_town, patterns_skill, patterns_jformat, patterns_jtype
from dict_for_model import dict_i_jformat, dict_job_types, all_skill_dict, dict_all_spec


pd.DataFrame.iteritems = pd.DataFrame.items
#Отображение колонок и строк в VScode
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Загрузка словарей, потом заменяется на SQL / Python запросы к БД
job_formats_dict = pd.read_csv("for_de/dict/job_formats.csv")
languages_dict = pd.read_csv("for_de/dict/languages.csv")
skills_dict = pd.read_csv("for_de/dict/skills.csv")
companies_dict = pd.read_csv("for_de/dict/companies.csv")
job_types_dict = pd.read_csv("for_de/dict/job_types.csv")
specialities_dict = pd.read_csv("for_de/dict/specialities.csv")
towns_dict = pd.read_csv("for_de/dict/towns.csv")
sources_dict = pd.read_csv("for_de/dict/sources.csv")



raw_sber = pd.read_csv("data/raw_sber_202311111958.csv")



class Data_preprocessing():

    def __init__(self):
        '''
        Инициализация моделей, подключение к БД
        '''
        # Подключение к БД / подгрузка датафрейма из БД (сделаем вместе с DE),
        self.vacancies = pd.DataFrame(columns = ['id', 'version', 'url', 'title', 'salary_from', 'salary_to', 
                                                 'experience_from', 'experience_to', 'description', 'company_id',
                                                 'source_id', 'publicated_at'])

        # Инициализация моделей на каждую колонку на core
        self.nlp = spacy.load('ru_core_news_lg')
        self.nlp_lem = spacy.load('ru_core_news_lg', disable=['parser', 'ner'])
        self.matcher_town = Matcher(self.nlp.vocab)
        self.matcher_skill = Matcher(self.nlp.vocab)
        self.matcher_jformat = Matcher(self.nlp.vocab)
        self.matcher_jtype = Matcher(self.nlp.vocab)

        # Создание таблиц с id вакансии
        self.job_formats_vacancies = pd.DataFrame(columns = ['vacancy_id', 'job_format_id'])
        self.languages_vacancies = pd.DataFrame(columns = ['vacancy_id', 'language_id'])
        self.skills_vacancies = pd.DataFrame(columns = ['vacancy_id', 'skill_id'])
        self.job_types_vacancies = pd.DataFrame(columns = ['vacancy_id', 'job_type_id'])
        self.specialities_vacancies = pd.DataFrame(columns = ['vacancy_id', 'spec_id', 'concurrence_percent'])
        self.towns_vacancies = pd.DataFrame(columns = ['vacancy_id', 'town_id'])
        self.ds_search = pd.DataFrame(columns = ['vacancy_id', 'vector'])


    def description_lemmatization(self, text):
        '''
        Загрузка датафрейма для лемматизации текста
        '''
        text = re.sub(r'[^\w\s]', ' ', text)
        doc = self.nlp_lem(text)
        processed = " ".join([token.lemma_ for token in doc])

        return processed


    def description_lemmatization_add(self, dataframe):
        # Объединяем все строки для поиска в одну
        dataframe['all_search'] = dataframe['towns'].astype(str) + ' ' + dataframe['description'].astype(str) + ' ' + dataframe['job_type'].astype(str) + ' ' + dataframe['job_format'].astype(str) + ' ' + dataframe['skills'].astype(str)
        dataframe['all_search'] = dataframe['all_search'].apply(self.description_lemmatization)


    def description_processing_town(self, dataframe, pat_town, towns_dict):
        '''
        Загрузка датафрейма и словаря patterns_town
        '''
        matcher_town = self.matcher_town
        matcher_town.add("TOWN_PATTERNS", pat_town)
        dataframe['town_search'] = dataframe['towns'].astype(str) + ' ' + dataframe['skills'].astype(str)
        
        for i_town in range(dataframe.shape[0]):
            dataframe.loc[i_town, 'town_search'] = re.sub(r'[^\w\s]', ' ', dataframe.loc[i_town, 'town_search'])
            doc = self.nlp(dataframe.loc[i_town, 'town_search'])
            matches = matcher_town(doc)

            list_town = []
            for match_id, start, end in matches:
                span = str(doc[start:end])
                list_town.append(span)
            fin_town = list(set(list_town))
            
            for element in fin_town:
                index = int(towns_dict.loc[towns_dict['clear_title'] == element.lower(), 'id'].iloc[-1]) # Можно заменить SQL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                self.towns_vacancies.loc[len(self.towns_vacancies.index)] = [dataframe.loc[i_town, 'vacancy_id'], index]

        self.towns_vacancies.drop_duplicates(inplace = True)


    def description_processing_skill(self, dataframe, pat_skill, all_skill_d, skills_dict):
        '''
        Загрузка датафрейма и словаря patterns_skill
        '''
        matcher_skill = self.matcher_skill
        matcher_skill.add("SKILL_PATTERNS", pat_skill)
        
        for i_skill in range(dataframe.shape[0]):
            doc = self.nlp(dataframe.loc[i_skill, 'all_search'])
            matches = matcher_skill(doc)
            list_skill = []
            for match_id, start, end in matches:
                span = str(doc[start:end])
                list_skill.append(span)
            fin_skill = list(set(list_skill))
            if not fin_skill: 
                fin_skill = ['не указан']
            

            for key, vals in all_skill_d.items():
                for val in vals:
                    for i in range(len(fin_skill)):
                        if fin_skill[i] == fin_skill[i]:
                            fin_skill[i] = fin_skill[i].replace(val, key)
                        else: 
                            fin_skill.remove(fin_skill[i])

            for element in fin_skill:
                try:
                    index = int(skills_dict.loc[skills_dict['title'] == element.lower(), 'id'].iloc[-1]) # Можно заменить SQL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    self.skills_vacancies.loc[len(self.skills_vacancies.index)] = [dataframe.loc[i_skill, 'vacancy_id'], index]
                except: pass

           
            #Для модели сохраним в датафрейм
            fin_str_skill = ''
            for el in fin_skill:
                fin_str_skill += str.lower(el)
                fin_str_skill += ','
            dataframe.loc[i_skill, 'skill_clean'] = fin_str_skill
        

        self.skills_vacancies.drop_duplicates(inplace = True)

        
    def description_processing_jformat(self, dataframe, pat_jformat, d_i_jformat, job_form_dict):
        '''
        Загрузка датафрейма и словаря patterns_jformat
        '''
        matcher_jformat = self.matcher_jformat
        matcher_jformat.add("JFORMAT_PATTERNS", pat_jformat)

        for i_jformat in range(dataframe.shape[0]):
            doc = self.nlp(dataframe.loc[i_jformat, 'all_search'])
            matches = matcher_jformat(doc)
            list_jformat = []
            for match_id, start, end in matches:
                span = str(doc[start:end])
                list_jformat.append(span)
            fin_jformat = list(set(list_jformat))
            if not fin_jformat: 
                fin_jformat = ['не указан']

            for key, vals in d_i_jformat.items():
                for val in vals:
                    for i in range(len(fin_jformat)):
                        fin_jformat[i] = fin_jformat[i].replace(val, key)

            for element in fin_jformat:
                index = int(job_form_dict.loc[job_form_dict['title'] == element.lower(), 'id'].iloc[-1]) # Можно заменить SQL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                self.job_formats_vacancies.loc[len(self.job_formats_vacancies.index)] = [dataframe.loc[i_jformat, 'vacancy_id'], index]


    def description_processing_jtype(self, dataframe, pat_jtype, job_type_dict, d_job_types):
        '''
        Загрузка датафрейма и словаря patterns_jtype
        '''
        matcher_jtype = self.matcher_jtype
        matcher_jtype.add("JTYPE_PATTERNS", pat_jtype)

        for i_jtype in range(dataframe.shape[0]):
            doc = self.nlp(dataframe.loc[i_jtype, 'all_search'])
            matches = matcher_jtype(doc)
            list_jtype = []
            for match_id, start, end in matches:
                span = str(doc[start:end])
                list_jtype.append(span)
            fin_jtype = list(set(list_jtype))

            if not fin_jtype: 
                fin_jtype = ['не указан']

            for key, vals in d_job_types.items():
                for val in vals:
                    for i in range(len(fin_jtype)):
                        fin_jtype[i] = fin_jtype[i].replace(val, key)

            for element in fin_jtype:
                index = int(job_type_dict.loc[job_type_dict['title'] == element.lower(), 'id'].iloc[-1]) # Можно заменить SQL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                self.job_types_vacancies.loc[len(self.job_types_vacancies.index)] = [dataframe.loc[i_jtype, 'vacancy_id'], index]


    # Опыт и зарплата. Пока нет конечного решения

    def clustering_specialties():
        pass


    def save_dataframe(self, dataframe):
        for num in range(dataframe.shape[0]):
            str_for_vacancies = {'id': (dataframe.loc[num, 'vacancy_id']),
                                 'version': (dataframe.loc[num, 'version_vac']),
                                 'url': (dataframe.loc[num, 'vacancy_url']),
                                 'title': (dataframe.loc[num, 'vacancy_name']),
                                 'salary_from': (dataframe.loc[num, 'salary_from']),
                                 'salary_to': (dataframe.loc[num, 'salary_to']),
                                 'experience_from': (dataframe.loc[num, 'exp_from']),
                                 'experience_to': (dataframe.loc[num, 'exp_to']),
                                # 'description': (dataframe.loc[num, 'description']),
                                 'company_id': (1),
                                 'source_id': (1),
                                 'publicated_at': dataframe.loc[num, 'date_created']}

            self.vacancies = self.vacancies._append(str_for_vacancies, ignore_index=True)


        

test = Data_preprocessing()
test.description_lemmatization_add(raw_sber)
test.description_processing_town(raw_sber, patterns_town, towns_dict)
test.description_processing_skill(raw_sber, patterns_skill, all_skill_dict, skills_dict)
test.description_processing_jformat(raw_sber, patterns_jformat, dict_i_jformat, job_formats_dict)
test.description_processing_jtype(raw_sber, patterns_jtype, job_types_dict, dict_job_types)


test.save_dataframe(raw_sber)



# Сохранить

test.towns_vacancies.to_csv(r'for_de/id-id/towns_vacancies.csv', index=False)
test.skills_vacancies.to_csv(r'for_de/id-id/skills_vacancies.csv', index=False)
test.job_formats_vacancies.to_csv(r'for_de/id-id/job_formats_vacancies.csv', index=False)
test.job_types_vacancies.to_csv(r'for_de/id-id/job_types_vacancies.csv', index=False)
test.languages_vacancies.to_csv(r'for_de/id-id/languages_vacancies.csv', index=False)
test.specialities_vacancies.to_csv(r'for_de/id-id/specialities_vacancies.csv', index=False)
test.ds_search.to_csv(r'for_de/id-id/ds_search.csv', index=False)

test.vacancies.to_csv(r'for_de/id-id/vacancies.csv', index=False)