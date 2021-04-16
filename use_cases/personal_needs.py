import use_cases.utils.textools as tt
import pandas as pd
import numpy as np
import re, os


def get_dialogues_info(frame):
    # Init Pipeline
    frame['Grupo'] = tt.check_nan(frame['Grupo'])

    frames = []
    for k in range(1, 6):
        partial = pd.DataFrame()
        partial['diag_id'] = frame['ID Archivo']
        partial['name'] = tt.to_unicode(frame['P3_{}_A'.format(k)])
        partial['exp'] = tt.to_unicode(frame['P3_{}_B'.format(k)])

        frames.append(partial)

    needs = pd.concat(frames)
    needs['name_tokens'] = tt.tokenize(needs['name'])
    needs['exp_tokens'] = tt.tokenize(needs['exp'])
    needs['macro'] = needs['name']
    diag_groups = needs.groupby('diag_id')
    needs['priority'] = [i for g, f in diag_groups for i in range(f.shape[0])]
    needs    = needs[['diag_id', 'name', 'name_tokens',
    'exp', 'exp_tokens', 'macro', 'priority']]
    needs = needs[~needs['name'].isna()]
    needs = needs.replace({'NR': '', 'nr':'', 'nan':''})
    return needs

def get_individuals_info(frame, frame_online):

    frames = []
    for i in range(1, 4):
        p1 = frame[['id', 'p3_1_a', 'p3_1_b']]
        p1.columns = ['ind_id', 'name', 'exp']

        p2 = frame_online[[
        'RUN',
        '1 >> Necesidades que enfrento personalmente o que existen en mi hogar o familia'.format(i),
        '1 >> Explique lo mencionado.2'.format(i)]]
        p2.columns = ['ind_id', 'name', 'exp']

        p1['name'] =  tt.to_unicode(p1['name'])
        p1['exp'] =  tt.to_unicode(p1['exp'])
        p1['priority'] = np.ones(p1.shape[0])*i
        p2['name'] =  tt.to_unicode(p2['name'])
        p2['exp'] =  tt.to_unicode(p2['exp'])
        p2['priority'] = np.ones(p2.shape[0])*i

        p1['is_online'] = np.zeros(p1.shape[0])
        p2['is_online'] = np.ones(p2.shape[0])

        p = pd.concat([p1, p2])


        frames.append(p)

    needs = pd.concat(frames)

    needs['name_tokens'] = tt.tokenize(needs['name'])
    needs['exp_tokens'] = tt.tokenize(needs['exp'])
    needs['macro'] = needs['name']

    needs    = needs[['ind_id', 'name', 'name_tokens',
    'exp', 'exp_tokens', 'macro', 'priority', 'is_online']]
    needs = needs[~needs['name'].isna()]
    needs = needs.replace({'NR': '', 'nr':'', 'nan':''})

    return needs

def create_table_personal_needs(frame, indv_frame, indv_online_frame):
    need_diag = get_dialogues_info(frame)
    need_ind = get_individuals_info(indv_frame, indv_online_frame)

    need_diag['is_online'] = np.zeros(need_diag.shape[0])

    need_table = pd.concat([need_diag, need_ind])
    need_table['is_online'] = need_table['is_online'].astype(int)

    need_table = need_table.fillna('')
    need_table = need_table.replace({'nr':'','nan':'', 'NR':'', 'NaN':'', np.nan:''})
    need_table = need_table[need_table['name'] != '']
    need_table['id'] = range(0, need_table.shape[0])
    need_table = need_table[['id', 'diag_id', 'ind_id', 'name', 'name_tokens',
                             'exp', 'exp_tokens', 'macro', 'priority',
                             'is_online']]
    return need_table
