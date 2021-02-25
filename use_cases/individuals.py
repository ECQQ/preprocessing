import multiprocessing
import textools as tt
import pandas as pd
import numpy as np
import re, os

from joblib import Parallel, delayed


def process_individuals():
    # Init Pipeline
    num_cores = multiprocessing.cpu_count()

    # Emotions
    emo_verified = pd.read_csv('./datos/emotions_verified.csv')
    
    #Open files as DF
    root = './datos/Individuales'
    files = [x for x in os.listdir(root) if x.endswith('.csv')]

    p1_homologada = pd.read_csv(os.path.join(root, files[0]))
    consultas = pd.read_csv(os.path.join(root, files[1]))
    p4_orden_cuestionario = pd.read_csv(os.path.join(root, files[2]))
    online = pd.read_csv(os.path.join(root, files[3]))

    # Getting information from individuals rows 
    info_cols = ['id', 'fecha encuesta', 'edad', 'región', 'comuna', 'educ_entrevistado']
    info_indi = consultas[info_cols]
    info_indi.columns = ['indiv_id', 'date', 'age', 'region', 'comuna', 'level']
    info_indi = info_indi.replace({'nr': 'NR'})

    # Run script for each froup
    import time
    t0 = time.time()

    # Iterates over individual ids
    all_emotions = []
    all_ids = []
    all_explanations = []
    all_contrib_text = []
    all_contrib_token = []
    k = 0 
    for ind_id in info_indi['indiv_id']:
        partial = p1_homologada[p1_homologada['id'] == ind_id]

        # Question 1
        for i in range(1, 4):
            p_a = partial['p1_{}_a'.format(i)]
            p_b = partial['p1_{}_b'.format(i)]

            p_a = tt.tokenize(p_a)
            p_a = tt.check_spelling(p_a)
            p_a = tt.equivalent_words(p_a, values=emo_verified)
            
            p_b = tt.tokenize(p_b)
            p_b = tt.check_spelling(p_b)
            p_b = tt.equivalent_words(p_b, values=emo_verified)
            

            if len(p_a.values[0][0]) == 1:
                all_emotions+=list(*p_a.values[0])
            else:
                all_emotions+=list(p_a.values[0])

            all_ids += [ind_id]*p_a.shape[0]
            all_explanations += list(p_b.values[0])

        # Question 5
        contribution = consultas[consultas['id'] == ind_id]['p5']
        cont_token = tt.tokenize(contribution)
        cont_token = tt.check_spelling(cont_token)
        contribution = tt.check_spelling(contribution)

        all_contrib_text += list(contribution.values)
        all_contrib_token += list(cont_token.values)

        if k == 3: break
        k+=1

    df_0 = pd.DataFrame()
    df_0['raw'] = all_emotions
    df_0['explanation'] = all_explanations
    df_0['name'] = [x if not isinstance(x, list) else 'NR' for x in all_emotions]
    df_0 = df_0.replace('nr', 'NR')
    df_0['macro'] = ['NR']*len(all_emotions)
    df_0 = df_0.reset_index()
    df_0 = df_0.rename(columns={'index':'emo_id'})

    df_1 = pd.DataFrame()
    df_1['ind_id'] = all_ids
    df_1['emo_id'] = df_0['emo_id'].values

    df_2 = pd.DataFrame()
    df_2['text'] = all_contrib_text
    df_2['token'] = all_contrib_token
    df_2['macro'] = ['NR']*len(all_contrib_text)
    df_2 = df_2.reset_index()
    df_2 = df_2.rename(columns={'index':'con_id'})

    df_3 = pd.DataFrame()
    df_3['ind_id'] = info_indi['indiv_id']
    df_3['con_id'] = df_2['con_id']

    df_0.to_csv('./nuevos_datos/individuals/emotions.csv', index=False)
    df_1.to_csv('./nuevos_datos/individuals/individual_emotions.csv', index=False)
    df_2.to_csv('./nuevos_datos/individuals/contributions.csv', index=False)
    df_3.to_csv('./nuevos_datos/individuals/individual_constribution.csv', index=False)
    info_indi.to_csv('./nuevos_datos/individuals/individuals.csv', index=False)

