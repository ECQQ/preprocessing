import use_cases.utils.textools as tt
import pandas as pd
import numpy as np
import re, os



def get_dialogues_info():
    pass

def get_individual_info():
    pass


def create_table_emotions(frame):

    question_cols = [x for x in frame.columns if re.search(r'P1_\d_[A|B]', x)]

    emo_list, explanations = [], []
    emo_token, exp_token = [], []
    file_ids = []
    for column in question_cols:
        if column.endswith('A'):
            name = tt.to_unicode(frame[column])
            emo_token.append(tt.tokenize(name))
            emo_list.append(name)
            file_ids.append(frame['ID Archivo'])

        elif column.endswith('B'):
            exp = tt.to_unicode(frame[column])
            explanations.append(exp)
            exp_token.append(tt.tokenize(exp))

    file_ids = pd.concat(file_ids)
    emo_token = pd.concat(emo_token)
    emo_list = pd.concat(emo_list)
    explanations = pd.concat(explanations)
    exp_token = pd.concat(exp_token)

    df_emo = pd.DataFrame()
    df_emo['diag_id'] = file_ids
    df_emo['name'] = emo_list
    df_emo['name_token'] = emo_token
    df_emo['macro'] = emo_list
    df_emo['text'] = explanations
    df_emo['text_tokens'] = exp_token
    cond  = ~df_emo['name'].isna()
    df_emo = df_emo[cond]
    df_emo = df_emo.replace({'nr':''})

    return df_emo
