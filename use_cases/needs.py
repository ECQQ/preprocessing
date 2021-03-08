import multiprocessing
import textools as tt
import pandas as pd
import numpy as np
import re, os

from joblib import Parallel, delayed

def process_one(row, k):
    re_0 = r'P4'

    frame_cols  = [x for x in row.index if re.search(re_0, x)]

    text_response = []
    token_response = []
    final = None


    #iterate over the rows 
    tuples = []

    for k in range(1, 6):
        fide       = row['ID Archivo']
        need       = row['P4_{}_A'.format(k)]
        rol_estado = row['P4_{}_B'.format(k)]
        otro       = row['P4_{}_C'.format(k)]
        rol_otro   = row['P4_{}_D'.format(k)]

        need = tt.to_unicode(need)
        need = " ".join(need.split())

        rol_estado = tt.to_unicode(rol_estado)
        rol_estado = " ".join(rol_estado.split())
        if '-' in rol_estado:
            roles_estado = rol_estado.split('-')
            for r in roles_estado:
                tuples.append([fide, need, 'estado', r])
        else:
            tuples.append([fide, need, 'estado', rol_estado])

        otro = tt.to_unicode(otro)
        otro = re.sub(r'\W+', ' ', otro)
        if 'respuesta sin completar' in otro or pd.isna(otro) or otro == 'nan':
            otro = 'NR'

        rol_otro = tt.to_unicode(rol_otro)
        rol_otro = " ".join(rol_otro.split())
        rol_otro = rol_otro.replace('-', '')
        if '-' in rol_otro:
            rol_otro = rol_otro.split('-')
            for r in rol_otro:
                tuples.append([fide, need, otro, r])
        else:
            tuples.append([fide, need, otro, rol_otro])

    df = pd.DataFrame(np.array(tuples), 
            columns=['file_id', 'name', 'actor', 'role'])
    df = df[df['name'] != 'nan']
    df = df[~df['name'].isna()]
    df['name'] = tt.check_spelling(df['name'])
    df['actor'] = tt.equivalent_words(df['actor'])
    df['role_token'] = tt.tokenize(df['role'])
    df = df.replace({'nan':'NR'})
    return df
    
def process_needs(frame):
    # Init Pipeline
    num_cores = multiprocessing.cpu_count()

    # Run script for each froup
    allframes = Parallel(n_jobs=num_cores)(delayed(process_one)(row, k) \
                    for k, row in frame.iterrows())

    needs = pd.concat(allframes)
    needs['need_id'] = range(0, needs.shape[0])
    needs['macro'] = needs['name'] 
    needs['role_token'] = tt.equivalent_words(needs['role_token'])
    needs.to_csv('./nuevos_datos/dialogues/needs.csv', index=False)


    persons = pd.read_csv('./nuevos_datos/dialogues/persons.csv')

    persons = persons[['person_id', 'file_id']]


    ids_1, ids_2 = [], []
    for k, p in persons.iterrows():
        particular = needs[needs['file_id'] == p['file_id']]['need_id']
        
        ids_1+=[p['person_id']]*len(particular)
        ids_2+=list(particular.values)


    intermidiate = pd.DataFrame()
    intermidiate['person_id'] = ids_1
    intermidiate['need_id'] = ids_2
    intermidiate.to_csv('./nuevos_datos/dialogues/persons_needs.csv')