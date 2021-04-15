import multiprocessing
import use_cases.utils.textools as tt
import pandas as pd
import numpy as np
import re, os

from joblib import Parallel, delayed

def create_table_contributions(frame):
    # Init Pipeline
    frame['Grupo'] = tt.check_nan(frame['Grupo'])

    frames = []
    for i in range(1, 6):
        col = frame[['ID Archivo', 'P5_{}'.format(i)]]
        col.columns = ['diag_id', 'text']
        col['text'] = tt.to_unicode(col['text'])
        col['text_tokens'] = tt.tokenize(col['text'])
        col['macro'] = col['text']
        frames.append(col)

    table = pd.concat(frames)
    table['id'] = range(table.shape[0])
    table = table[['id', 'diag_id', 'text', 'text_tokens', 'macro']]

    return table
