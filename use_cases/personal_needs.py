import use_cases.utils.textools as tt
import pandas as pd
import numpy as np
import re, os


def create_table_personal_needs(frame):
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

    needs['id'] = range(0, needs.shape[0])
    needs['name_tokens'] = tt.tokenize(needs['name'])
    needs['exp_tokens'] = tt.tokenize(needs['exp'])
    needs['macro'] = needs['name']
    diag_groups = needs.groupby('diag_id')
    needs['priority'] = [i for g, f in diag_groups for i in range(f.shape[0])]
    needs    = needs[['id','diag_id', 'name', 'name_tokens',
                      'exp', 'exp_tokens', 'macro', 'priority']]
    needs = needs[~needs['name'].isna()]
    needs = needs.replace({'NR': '', 'nr':'', 'nan':''})
    return needs
