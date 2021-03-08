import multiprocessing
import textools as tt
import pandas as pd
import numpy as np
import re, os

from joblib import Parallel, delayed

def process_one(row, k):
    re_0 = r'P5_\d'

    frame_cols  = [x for x in row.index if re.search(re_0, x)]

    text_response = []
    token_response = []
    final = None
    for column in frame_cols: 
        single_col = pd.Series(row[column])
        single_col = tt.tokenize(single_col)
        single_col = single_col.values[0]
        if 'nan' not in single_col:
            token_response.append(single_col)
            text_response.append(row[column])

    # TEXT RESPONSE CLEANING
    text_response = pd.Series(text_response)
    text_response = tt.check_spelling(text_response)
    text_response = tt.equivalent_words(text_response)
    # TOKEN BASED RESPONSE CLEANING
    token_response = pd.Series(token_response)
    token_response = tt.check_spelling(token_response)
    # FILTERING NAN VALUES
    req_serie_text  = text_response[(~pd.isna(text_response)) & (text_response != 'nan')]
    req_serie_token = token_response[(~pd.isna(token_response)) & (token_response != 'nan')]

    id_file   = row['ID Archivo'] 
    # AT LEAST ONE ANSWER
    if req_serie_token.shape[0] >= 1:
        serie = pd.Series([id_file]*len(req_serie_token))
        final = pd.concat([serie, req_serie_text, req_serie_token], 1)
        final.columns = ['id_file', 'text' , 'tokens']

    return final
    
def process_contributions(frame):
    # Init Pipeline
    num_cores = multiprocessing.cpu_count()

    # Run script for each froup
    import time
    t0 = time.time()
    allframes = Parallel(n_jobs=num_cores)(delayed(process_one)(row, k) \
                    for k, row in frame.iterrows())

    contributions = pd.concat(allframes)
    contributions['con_id'] = range(0, contributions.shape[0])
    contributions['tokens'] = tt.equivalent_words(contributions['tokens'])

    contributions.to_csv('./nuevos_datos/contributions.csv', index=False)