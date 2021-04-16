import use_cases.utils.textools as tt
import pandas as pd
import numpy as np
import re, os



def get_dialogues_info(frame):
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
    df_emo['source_id'] = file_ids
    df_emo['name'] = emo_list
    df_emo['name_tokens'] = emo_token
    df_emo['macro'] = emo_list
    df_emo['exp'] = explanations
    df_emo['exp_tokens'] = exp_token
    cond  = ~df_emo['name'].isna()
    df_emo = df_emo[cond]
    df_emo = df_emo.replace({'nr':''})
    return df_emo

def get_individual_info(frame_path, frame_online):
    frame = pd.read_excel(frame_path, 'P1_HOMOLOGADA')

    frames = []
    for i in range(1, 3):
        handwritten = frame[['id', 'p1_{}_a'.format(i), 'p1_{}_b'.format(i)]]
        handwritten.columns = ['source_id', 'name', 'exp']

        online = frame_online[['RUN',
                               '{} >> Emociones / Sentimientos / Sensaciones'.format(i),
                               '{} >> Explique lo mencionado'.format(i)]]
        online.columns = ['source_id', 'name', 'exp']

        handwritten['is_online'] = np.zeros(handwritten.shape[0])
        online['is_online'] = np.ones(online.shape[0])

        p = pd.concat([handwritten, online])
        p['name'] = tt.to_unicode(p['name'])
        p['exp'] = tt.to_unicode(p['exp'])
        frames.append(p)

    table = pd.concat(frames)
    table = table.fillna('')
    table = table.replace({'nr':'','nan':'', 'NR':'', 'NaN':'', np.nan:''})
    table['name_tokens'] = tt.tokenize(table['name'])
    table['exp_tokens'] = tt.tokenize(table['exp'])
    return table


def create_table_emotions(frame, frame_ind_path, frame_ind_online):

    emo_diag = get_dialogues_info(frame)
    ind_diag = get_individual_info(frame_ind_path, frame_ind_online)

    emo_diag['is_dialogue'] = np.ones(emo_diag.shape[0])
    emo_diag['is_online'] = np.zeros(emo_diag.shape[0])

    ind_diag['is_dialogue'] = np.zeros(ind_diag.shape[0])

    table = pd.concat([emo_diag, ind_diag])
    table['is_online'] = table['is_online'].astype(int)
    table['is_dialogue'] = table['is_dialogue'].astype(int)

    table = table.fillna('')
    table = table.replace({'nr':'','nan':'', 'NR':'', 'NaN':'', np.nan:''})
    table = table[table['name'] != '']
    table['id'] = range(0, table.shape[0])

    return table
