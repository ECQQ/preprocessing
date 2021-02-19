import multiprocessing
import textools as tt
import pandas as pd
import re, os

from joblib import Parallel, delayed


def get_unified_emotions(row, question_cols):
    """ Clean each column and store all emotions in a single list

    Args:
        row ([pandas Serie]): A row from the main dataframe (It represent a group) 
        question_cols ([type]): Selected column to be analyzed

    Returns:
        [pandas Serie]: A pandas serie with the clean emotions
    """
    words = []
    for column in question_cols:       
        single_col = pd.Series(row[column])
        single_col = tt.tokenize(single_col)
        words += single_col.values[0]
    single_col = pd.Series(words)
    single_col = tt.check_spelling(single_col)
    single_col = tt.equivalent_words(single_col)
    single_col = single_col.dropna()
    single_col = single_col[single_col!='nan']
    return single_col

def get_members_info(row, max_member=30):
    """ Extract group information such as location, age, sex...

    Args:
        row ([pandas Serie]): A row from the main dataframe (It represent a group) 
        max_member (int): Max number of members

    Returns:
        [pandas Serie]: Pandas dataframe with the collected information
    """
    comunas_df = pd.read_csv('./datos/comuna_region.csv')
    info = []
    for i in range(1, max_member+1):
        age     = tt.check_nan(row['LP_EDAD{}'.format(i)])
        sex     = tt.check_nan(row['LP_COD_SEXO_P{}'.format(i)])
        level   = tt.check_nan(tt.to_unicode(row['LP_COD_NIVEL_P{}'.format(i)]))
        comuna  = tt.check_nan(row['LP_COMUNA{}'.format(i)])
        region  = tt.check_nan(comunas_df[comunas_df['Comuna'] == tt.to_unicode(comuna)]['Region'])
        file_id = tt.check_nan(row['ID Archivo'])
        info.append([file_id, age, sex, level, comuna, region])
    df = pd.DataFrame(info)
    df.columns = ['file_id', 'age', 'sex', 'level', 'comuna', 'region']
    df = df[df['age'] != 'NR']
    df['age'] = df['age'].astype(int)
    df = tt.stratify_frame_by_age(df)

    return df

def get_group_info(row):
    """Capture important information about the group

    Args:
        row (pandas Serie): A row from the main dataframe (It represent a group) 

    Returns:
        [pandas Serie]: Same sarie without nonrelevant data
    """
    selected = row[['ID Archivo', 'Fecha', 'Hora Inicio', 
                'Hora Termino', 'Lugar', 'Dirección', 
                'Comuna', 'Region', 'Participantes', 
                'Grupo']]
    selected['Grupo'] = tt.check_nan(selected['Grupo'])
    return selected


def process_one(row, k):
    question_cols = [x for x in row.index if re.search(r'P1_\d_A', x)]

    emotions = get_unified_emotions(row, question_cols)
    gmembers = get_members_info(row)
    ginfo    = get_group_info(row)

    return emotions, gmembers, ginfo

def process_emotions(frame):
    # Init Pipeline
    num_cores = multiprocessing.cpu_count()

    # Run script for each froup
    allframes = Parallel(n_jobs=num_cores)(delayed(process_one)(row, k) \
                    for k, row in frame.iterrows())

    # Filter None values from distributed computation
    emotions = [f[0] for f in allframes if f[0] is not None]
    gmembers = [f[1] for f in allframes if f[1] is not None]
    ginfo    = [f[2] for f in allframes if f[2] is not None]

    # Concat distributed results
    emotions_concat = pd.concat(emotions).reset_index().iloc[:, 1:]
    gmembers_concat = pd.concat(gmembers).reset_index().iloc[:, 1:].reset_index()
    gmembers_concat = gmembers_concat.rename(columns={'index': 'person_id'})
    ginfo_concat    = pd.concat(ginfo, 1).reset_index().iloc[:, 1:].T.reset_index()
    ginfo_concat.columns = ['diag_id', 'file_id', 'date', 'init_time', 'end_time', 
                            'location', 'address', 'comuna', 'region', 'n_members', 'group_name']
    
    # Replace equivalent emotions
    emotions_concat = tt.equivalent_words(emotions_concat)
    emotions_concat = pd.DataFrame(emotions_concat[0].unique(), columns=['name'])    
    emotions_concat = emotions_concat.reset_index()
    emotions_concat = emotions_concat.rename(columns={'index': 'emo_id'})

    # Get index of aociated emotions
    emo_indices = []
    grp_indices = []
    for i, e in enumerate(emotions):
        file_id = ginfo[i]['ID Archivo']
        for ee in e:
            s = emotions_concat[emotions_concat['name'] == ee]['emo_id']
            emo_indices.append(s.values[0])
            grp_indices.append(file_id)

    # Create emo_user ids table
    user_emotion = pd.DataFrame()
    user_emotion['emo_id'] = emo_indices
    user_emotion['file_id'] = grp_indices
    emo_user = pd.merge(user_emotion, gmembers_concat, on=['file_id'])
    emo_user = emo_user[['emo_id', 'person_id']]

    # Saving tables
    emo_user.to_csv('./nuevos_datos/emo_user.csv', index=False)
    emotions_concat.to_csv('./nuevos_datos/emotions.csv', index=False)
    gmembers_concat.to_csv('./nuevos_datos/persons.csv', index=False)
    ginfo_concat.to_csv('./nuevos_datos/dialogue.csv', index=False)