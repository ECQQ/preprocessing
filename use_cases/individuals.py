import pandas as pd
import multiprocessing
import os, re
import numpy as np
import use_cases.utils.textools as tt
import unidecode
import math

from joblib import Parallel, delayed

comunas = pd.read_csv('./data/comuna.csv')
comunas_name = np.array([unidecode.unidecode(x).lower() for x in comunas['name'].to_numpy()],dtype=str)
comunas_id = np.array(comunas['id'].to_numpy(), dtype=int)
comuna_code = dict(zip(comunas_name, comunas_id))

comuna_code_2 = dict(zip(comunas_id, comunas_name))

comunas_fix = {
    'isla  de pascua': 'isla de pascua',
    'trehuaco' : 'treguaco',
    'coccepcion' : 'concepcion',
    'conce' : 'concepcion',
    'concepcion.' : 'concepcion',
    'santiago centro' : 'santiago',
    'caleta tortel' : 'tortel',
    'puente' : 'puente alto',
    'san vicente de tagua tagua' : 'san vicente',
    'san vicente tagua tagua' : 'san vicente',
    'marchigue' : 'marchihue',
    'coihaique' : 'coyhaique',
    'coyihaque' : 'coyhaique',
    'haulpen' : 'hualpen',
    'vina': 'vina del mar',
    'la  serena': 'la serena',
    'huechurabs' : 'huechuraba',
    'providenica' : 'providencia',
    'providenca' : 'providencia',
    'cowuimbo' : 'coquimbo',
    'comuna de putre' : 'putre',
    'x region, chile' : 'nr',
    'v region' : 'nr',
    'alto hospicii' : 'alto hospicio',
    'san miguel.' : 'san miguel',
    'pozo amonte' : 'pozo almonte',
    'til til' : 'tiltil',
    'qta normal' : 'quinta normal',
    'quinta norma' : 'quinta normal',
    'milina' : 'molina',
    'batuco' : 'lampa',
    'la visterna' : 'la cisterna',
    '"puerto montt' : 'puerto montt',
    'extranjero' : 'nr',
    'cerrillos.' : 'cerrillos',
    'maipu (mientras)..' : 'maipu',
    'colchagua': 'nr',
    'san antonio comuna de cartagena': 'cartagena',
    'quemchi chiloe-' : 'quemchi',
    'rocas de santo domingo' : 'santo domingo',
    'la calera' : 'calera',
    'coyhique' : 'coyhaique',
    'cancun' : 'nr',
    'estados unidos' : 'nr',
    'gladstone' : 'nr',
    'qjillota' : 'quillota',
    'pac' : 'pedro aguirre cerda',
    'paihuano' : 'paiguano',
    'puerto aysen' : 'aysen',
    'provincia' : 'nr',
    'santioago' : 'santiago',
    'quilpue  (belloto)' : 'quilpue'
}

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

def fix_location_online(x):
    if pd.isna(x['Comuna']):
        if pd.isna(x['Comuna.1']):
            x['Comuna'] = ''
        else:
            x['Comuna'] = x['Comuna.1']
    try:
        x['Comuna'] = comuna_code[unidecode.unidecode(x['Comuna']).lower()]        
    except KeyError:
        x['Comuna'] = comuna_code[comunas_fix[unidecode.unidecode(x['Comuna']).lower()]]

    return x

def fix_location(x):    
    if x['comuna'] == 'nr':
        x['comuna'] = 1

    if pd.isna(x['comuna']):
        x['comuna'] = 1

    return x

def get_age(x, col):
    if not pd.isna(x[col]):   
        try:
            x[col] = re.match(r'\d+', str(x[col])).group(0)   
        except AttributeError:
            x[col] = ''   
    else:
        x[col] = '' 
    return x 

def create_table_individuals(online, digi):
    # Init Pipeline

    # ============================================
    # Getting information from online individuals rows
    # ============================================
    
    online['id'] = ['{}'.format(k) for k in range(online.shape[0])]
    online = online.apply(lambda x: fix_location_online(x), 1)
    online = online.apply(lambda x: format_date(x, 'Submission Date'), 1)
    online = online.apply(lambda x: get_age(x, 'Edad'), 1)
    online = online[['id', 'Submission Date','Edad', 'Comuna', '1. ¿Cuál es el nivel de educación alcanzado por Usted?']]
    online['online'] = True
    online.columns = ['id', 'date', 'age', 'comuna_id', 'level', 'online']

    # ============================================
    # Getting information from digitalized individuals rows
    # ============================================

    digi['id'] =  ['{}'.format(k) for k in range(online.shape[0],online.shape[0] + digi.shape[0])]
    digi = digi.apply(lambda x: fix_location(x), 1) 
    digi = digi.apply(lambda x: get_age(x, 'edad'), 1)
    digi = digi.apply(lambda x: format_date(x, 'fecha encuesta'), 1)
    digi['educ_entrevistado'] = digi['educ_entrevistado'].replace(educ_dic)
    digi = digi[['id', 'fecha encuesta','edad', 'comuna', 'educ_entrevistado']]
    digi['online'] = False
    digi.columns = ['id', 'date', 'age', 'comuna_id', 'level', 'online']

    concat = pd.concat([digi, online]).reset_index().iloc[:, 1:]

    return concat
    
