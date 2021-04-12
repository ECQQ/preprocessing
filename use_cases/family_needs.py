import multiprocessing
import textools as tt
import pandas as pd
import numpy as np
import re, os

from joblib import Parallel, delayed

def process_one(row, k):
    re_0 = r'P3'

    frame_cols  = [x for x in row.index if re.search(re_0, x)]
    text_response = []
    token_response = []
    final = None


    #iterate over the rows 
    tuples = []

    for k in range(1, 6):
        fide       = row['ID Archivo']
        need       = row['P3_{}_A'.format(k)]
        explanation = row['P3_{}_B'.format(k)]


        need = tt.to_unicode(need)
        need = " ".join(need.split())

        explanation = tt.to_unicode(explanation)
        explanation = " ".join(explanation.split())

        tuples.append([fide, need, need, explanation, k])

    df = pd.DataFrame(np.array(tuples), 
            columns=['file_id', 'name', 'macro', 'explanation', 'priority'])
    df = df[df['name'] != 'nan']
    df = df[~df['name'].isna()]
    df['name'] = tt.equivalent_words(df['name'])
    df['exp_token'] = tt.tokenize(df['explanation'])
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

    needs.to_csv('./nuevos_datos/dialogues/personal_needs.csv', index=False)


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


    intermidiate.to_csv('./nuevos_datos/dialogues/persons_personal_needs.csv')