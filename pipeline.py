import multiprocessing
import textools as tt
import pandas as pd
import argparse
import re

from joblib import Parallel, delayed

def process_one(row, k):
    re_0 = r'P1_\d_A'
    re_1 = r'LP_EDAD'
    comunas_df = pd.read_csv('datos/comuna_region.csv')
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
            
            emo_serie = single_col[(~pd.isna(single_col)) & (single_col != 'nan')]
            person_id = re.search(r'\d+', m).group() 
            user_id = '{}_{}'.format(k, person_id)

            comuna = row['LP_COMUNA{}'.format(person_id)]
            comuna = row['Comuna'] if pd.isna(comuna) else comuna 
            comuna = tt.to_unicode(pd.Series(comuna)).values[0]
            try:
                region = comunas_df[comunas_df['Comuna'] == comuna]['Region'].values[0]
            except:
                region = -99

            sex = row['LP_COD_SEXO_P{}'.format(person_id)]
            level = tt.to_unicode(row['LP_COD_NIVEL_P{}'.format(person_id)])
            group = tt.to_unicode(row['Grupo'])
            id_file = row['ID Archivo']        
            emo_serie = emo_serie.reset_index()
            date_ = row['Fecha']  
            date_init = row['Hora Inicio']  
            date_end  = row['Hora Termino']  
            
            if emo_serie.shape[0] > 1:
                final = []
                for colp in [user_id, id_file, age, sex, comuna, region, 
                            level, group, date_, date_init, date_end]:
                
                    serie = pd.Series([colp]*len(emo_serie))
                    final.append(serie)
                final.append(emo_serie)               
                final = pd.concat(final, 1)
                final.columns = ['id_user', 'id_file', 'age', 'sex', 'comuna', 'region', 'education', 'group', 
                                'date', 'init_time', 'end_time', 'priority', 'emotion']
                finals_df.append(final)

    if len(finals_df)>1:
        finals_df = pd.concat(finals_df)
        return finals_df
    else:
        return None

def clean_text(frame):
    # Init Pipeline
    num_cores = multiprocessing.cpu_count()
    print('[INFO] file: {} - {} CPU'.format(opt.data, num_cores))
    comunas = pd.read_csv('./datos/comuna_region.csv', usecols=[0])

    # Regex patter to select some columns
    allframes = Parallel(n_jobs=num_cores)(delayed(process_one)(row, k) \
                    for k, row in frame.iterrows())

    allframes = [f for f in allframes if f is not None]
    new_data = pd.concat(allframes, 0)

    # Stratify 
    etiquetas = ['{}-{}'.format(n, n+15) for n in range(0, 60, 15)]
    etiquetas += ['>=60']
    range_values = [i for i in range(0, 61, 15)]
    range_values.append(100)
    new_data['age_range'] = pd.cut(new_data['age'], range_values, right=False, labels=etiquetas)
    new_data.to_csv('./nuevos_datos/emo_per_user.csv', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', type=str, default='c',
                        help='options: (c)leaning - (j)oin')
    parser.add_argument('--path', type=str, default='./runs/test',
                        help='project path')
    parser.add_argument('--data', type=str, default='./datos/Dialogo/BBDD_dialogos_final.csv',
                        help='raw data directory')
    parser.add_argument('--data2', type=str, default='',
                    help='additional data to be joined with --data')
    parser.add_argument('--debug', action='store_true', 
                        help='debugger mode')

    opt = parser.parse_args()
    
    # Reading data
    if opt.debug:
        chunksize = 10
        frame1 = pd.read_csv(opt.data, chunksize=chunksize, low_memory=False)
        for frame_1 in frame1: break
        if opt.data2 != '':
            frame2 = pd.read_csv(opt.data2, chunksize=chunksize, low_memory=False)
            for frame_2 in frame2: break
    else:
        frame_1 = pd.read_csv(opt.data, low_memory=False)
        if opt.data2 != '':
            frame_2 = pd.read_csv(opt.data2, low_memory=False)

    if opt.mode == 'c':
        clean_text(frame_1, frame_2)

    if opt.mode == 'j':
        which = ['code_comuna', 
                 'emotion_verified', 
                 'emotion_verified2']

        frame_3 = tt.combine_versions(frame_1, frame_2, 
                                      on='id_user', which=which)
        

        frame_3.to_csv('./nuevos_datos/emo_per_user_v4.csv', index=False)       

        frame_4  = frame_3.sample(n=1000)
        frame_4.to_csv('./nuevos_datos/emo_per_user_v4_tiny.csv', index=False)