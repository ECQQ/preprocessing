import multiprocessing
import textools as tt
import pandas as pd
import argparse
import re

from joblib import Parallel, delayed

def process_one(row, k):
    re_0 = r'P1_\d_A'
    re_1 = r'LP_EDAD'
    frame_cols  = [x for x in row.index if re.search(re_0, x)]
    member_cols = [x for x in row.index if re.search(re_1, x)]
    finals_df   = []
    for m in member_cols:
        age = row[m]
        if not pd.isna(age):
            words = []
            for column in frame_cols:       
                single_col = pd.Series(row[column])
                single_col = tt.tokenize(single_col)
                words += single_col.values[0]

            single_col = pd.Series(words)
            single_col = tt.check_spelling(single_col)
            single_col = tt.equivalent_words(single_col)                    

            emotions = single_col
            person_id = re.search(r'\d+', m).group() 
            user_id = '{}_{}'.format(k, person_id)

            comuna = row['LP_COMUNA{}'.format(person_id)]
            comuna = row['Comuna'] if pd.isna(comuna) else comuna 
            sex = row['LP_SEXO{}'.format(person_id)]
            level = row['LP_NIVEL{}'.format(person_id)]
            group = row['Grupo']
            id_file = row['ID Archivo']
            
            emo_serie = emotions[(~pd.isna(emotions)) & (emotions != 'nan')]
            emo_serie = emo_serie.reset_index()
            idu_serie = pd.Series([user_id]*len(emo_serie))
            age_serie = pd.Series([age]*len(emo_serie))
            sex_serie = pd.Series([sex]*len(emo_serie))
            lev_serie = pd.Series([level]*len(emo_serie))
            grp_serie = pd.Series([group]*len(emo_serie))
            idf_serie = pd.Series([id_file]*len(emo_serie))
            
            final = pd.concat([idf_serie, 
                                idu_serie,
                                grp_serie, 
                                age_serie, 
                                lev_serie, 
                                sex_serie, 
                                emo_serie], 1)
            
            final.columns = ['id_file', 'id_user', 'group', 'age', 'education', 'sex', 'priority', 'emotion']
            finals_df.append(final)
    
    new_df = pd.concat(finals_df)
    return new_df


def run_pipeline():

    # Init Pipeline
    num_cores = multiprocessing.cpu_count()
    print('[INFO] file: {} - {} CPU'.format(opt.data, num_cores))

    # Reading data
    comunas = pd.read_csv('./datos/comuna_region.csv', usecols=[0])
    if opt.debug:
        chunksize = 50
        dialogos_df = pd.read_csv(opt.data, chunksize=chunksize, low_memory=False)
        for frame in dialogos_df: break
    else:
        frame = pd.read_csv(opt.data, low_memory=False)
    
    # Regex patter to select some columns
    
    allframes = Parallel(n_jobs=num_cores)(delayed(process_one)(row, k) \
                    for k, row in frame.iterrows())

    new_data = pd.concat(allframes, 0)
    new_data.to_csv('./datos/Dialogo/emo_per_user.csv', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', type=str, default='c',
                        help='options: (c)leaning ')
    parser.add_argument('--path', type=str, default='./runs/test',
                        help='project path')
    parser.add_argument('--data', type=str, default='./datos/Dialogo/BBDD_dialogos_final.csv',
                        help='raw data directory')
    parser.add_argument('--debug', action='store_true', 
                        help='debugger mode')

    opt = parser.parse_args()

    run_pipeline()