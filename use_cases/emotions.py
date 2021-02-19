import textools as tt
import pandas as pd
import re


def process_one(row, k):
    re_0 = r'P1_\d_A'
    re_1 = r'LP_EDAD'
    
    comunas_df = pd.read_csv('../datos/comuna_region.csv')
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