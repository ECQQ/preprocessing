import use_cases.utils.textools as tt
import pandas as pd
import numpy as np
import re, os



def process_emotions(frame):

    question_cols = [x for x in frame.columns if re.search(r'P1_\d_[A|B]', x)]

    emo_list, explanations = [], []
    emo_token, exp_token = [], []
    file_ids = []
    for column in question_cols:
        if column.endswith('A'):
            col = tt.tokenize(frame[column])
            emo_token.append(col)
            emo_list.append(frame[column])
        elif column.endswith('B'):
            col = tt.tokenize(frame[column])
            exp_token.append(col)
            explanations.append(frame[column])
            file_ids.append(frame['ID Archivo'])

    file_ids = pd.concat(file_ids)
    emo_token = pd.concat(emo_token)
    emo_list = pd.concat(emo_list)
    explanations = pd.concat(explanations)
    exp_token = pd.concat(exp_token)

    df_emo = pd.DataFrame()
    df_emo['name'] = emo_list
    df_emo['token'] = emo_token
    df_emo['macro'] = emo_list
    cond  = ~df_emo['name'].isna()
    df_emo = df_emo[cond]

    df_exp = pd.DataFrame()
    df_exp['file_ids'] = file_ids
    df_exp['text'] = explanations
    df_exp['text_tokens'] = exp_token
    df_exp = df_exp[cond]

    # UNique emotions and ids
    unique_emotions = list(df_emo['name'].unique())

    df_emo['emo_id'] = df_emo['name'].apply(lambda x: unique_emotions.index(x))
    df_exp['emo_id'] = df_emo['emo_id']

    df_emo.drop_duplicates(subset="emo_id", keep="first", inplace=True)
    return df_emo, df_exp
