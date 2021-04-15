import pandas as pd
import multiprocessing
import os, re
import numpy as np

from use_cases.utils.comunas import comuna_code, comunas_fix, fix_location, fix_location_online
from use_cases.utils.textools import stratify_frame_by_age
educ_dic = {
    1 : 'Educación básica incompleta o inferior',
    2 : 'Básica completa',
    3 : 'Media incompleta (incluyendo Media Técnica)',
    4 : 'Media completa. Técnica incompleta',
    5 : 'Universitaria incompleta. Técnica completa',
    6 : 'Técnica completa',
    7 : 'Universitaria completa',
    8 : 'Post Grado (Master, Doctor o equivalente)',
    'nr': ''
}

def format_date(x, col):
    try:
        if x[col] == 'nr':
            x[col] = ''
        else:
            x[col] = pd.to_datetime(x[col], infer_datetime_format=True)
            x[col] = x[col].strftime('%d-%m-%Y')
    except:
        x[col] = ''
    return x

def get_age(x, col):
    if not pd.isna(x[col]):
        try:
            x[col] = re.match(r'\d+', str(x[col])).group(0)
        except AttributeError:
            x[col] = '0'
    else:
        x[col] = '0'
    return x

def create_table_individuals(online_survey, digi_survey):
    # Init Pipeline
    online = online_survey.copy()
    digi   = digi_survey.copy()
    # ============================================
    # Getting information from online individuals rows
    # ============================================
    online.sort_values('RUN', inplace=True)
    online.drop_duplicates(subset='RUN', keep=False, inplace=True)

    online = online.apply(lambda x: fix_location_online(x), 1)
    online = online.apply(lambda x: format_date(x, 'Submission Date'), 1)
    online = online.apply(lambda x: get_age(x, 'Edad'), 1)

    online = online[['RUN', 'Submission Date','Edad', 'Comuna', '1. ¿Cuál es el nivel de educación alcanzado por Usted?']]
    online['online'] = True
    online.columns = ['id', 'date', 'age', 'comuna_id', 'level', 'online']

    online = online[~online['age'].isna()]
    online['age'] = online['age'].apply(lambda x: x[:2])
    online['age'] = online['age'].astype(int)
    online = stratify_frame_by_age(online)

    # ============================================
    # Getting information from digitalized individuals rows
    # ============================================

    digi.sort_values('id', inplace=True)
    digi.drop_duplicates(subset='id', keep=False, inplace=True)

    digi = digi.apply(lambda x: fix_location(x), 1)
    digi = digi.apply(lambda x: get_age(x, 'edad'), 1)
    digi = digi.apply(lambda x: format_date(x, 'fecha encuesta'), 1)
    digi['educ_entrevistado'] = digi['educ_entrevistado'].replace(educ_dic)

    digi = digi[['id', 'fecha encuesta','edad', 'comuna', 'educ_entrevistado']]
    digi['online'] = False
    digi.columns = ['id', 'date', 'age', 'comuna_id', 'level', 'online']

    digi = digi[~digi['age'].isna()]
    digi['age'] = digi['age'].apply(lambda x: x[:2])
    digi['age'] = digi['age'].astype(int)
    digi = stratify_frame_by_age(digi)

    concat = pd.concat([digi, online]).reset_index().iloc[:, 1:]
    concat = concat.replace({0:'', 'nan':'', 'nr':''})

    return concat
