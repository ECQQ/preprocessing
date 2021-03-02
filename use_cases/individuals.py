import pandas as pd
import multiprocessing
import os, re
import numpy as np
import textools as tt

from joblib import Parallel, delayed

def format_date(x, col):
    try:
        if x[col] == 'nr':
            x[col] = 'NR'
        else:
            x[col] = pd.to_datetime(x[col], infer_datetime_format=True)
            x[col] = x[col].strftime('%d-%m-%Y')
    except:
        x[col] = 'NR'
    return x

def fix_location(x):

    if pd.isna(x['Comuna']):
        if pd.isna(x['Comuna.1']):
            x['Comuna'] = 'NR'
        else:
            x['Comuna'] = x['Comuna.1']

    if pd.isna(x['Región']):
        if pd.isna(x['Región.1']):
            x['Región'] = 'NR'
        else:
            x['Región'] = x['Región.1']

    return x
educ_dic = {'1': 'Educación básica incompleta o inferior',
            '2': 'Básica completa',
            '3': 'Media incompleta (incluyendo Media Técnica)',
            '4': 'Media completa. Técnica incompleta',
            '5': 'Universitaria incompleta. Técnica completa',
            '6': 'Técnica completa',
            '7': 'Universitaria completa',
            '8': 'Post Grado (Master, Doctor o equivalente)'}

priority_dic = {'nr':'NR',
                'Urgencia (solo una)':'NR',
                'urgente':'3',
                'No hacerle caso a sus opiniones y dejar en claro la neceidad de pruebas para que estas manifiesten sus opiniones':'NR',
                'fuerzas armadas en las calles':'NR',
                'segunda': '2',
                'primera':'1',
                'tercera':'3'}

def process_individuals():
    # Init Pipeline
    num_cores = multiprocessing.cpu_count()
    
    # Comuna code
    comuna_code = pd.read_csv('./datos/codigo_comunas.csv')
    comuna_code = dict(zip(np.array(comuna_code['Código'].values, dtype=str), comuna_code['Comuna'].values))

    # Emotions
    emo_verified = pd.read_csv('./datos/emotions_verified.csv')

    #Open files as DF
    root = './datos/Individuales'
    files = [x for x in os.listdir(root) if x.endswith('.csv')]

    p1_homologada = pd.read_csv(os.path.join(root, files[0]))
    consultas = pd.read_csv(os.path.join(root, files[1]))
    p4_orden_cuestionario = pd.read_csv(os.path.join(root, files[2]))
    online = pd.read_csv(os.path.join(root, files[3]))

    # p1_homologada = p1_homologada.sample(20)
    # consultas = consultas.sample(20)
    # p4_orden_cuestionario = p4_orden_cuestionario.sample(20)

    # Getting information from individuals rows 
    info_cols = ['id', 'fecha encuesta', 'edad', 'región', 'comuna', 'educ_entrevistado']
    info_indi = consultas[info_cols]
    info_indi.columns = ['indiv_id', 'date', 'age', 'region', 'comuna', 'level']
    info_indi = info_indi.apply(lambda x: format_date(x, 'date'), 1)
    info_indi['level'] = info_indi['level'].replace(educ_dic)
    info_indi = info_indi.replace({'nr': 'NR'})
    info_indi['comuna'] = info_indi['comuna'].replace(comuna_code)

    online['id'] = ['O{}'.format(k) for k in range(online.shape[0])]
    online_info = online.apply(lambda x: fix_location(x), 1)
    online_info = online_info[['id', 'Submission Date','Edad', 'Comuna', 'Región', '1. ¿Cuál es el nivel de educación alcanzado por Usted?']]
    online_info = online_info.apply(lambda x: format_date(x, 'Submission Date'), 1)
    online_info.columns = ['indiv_id', 'date', 'age', 'comuna', 'region', 'level']
    online_info['level'] = online_info['level'].apply(lambda x: x.replace('.', ''))

    info_concat = pd.concat([info_indi, online_info]).reset_index().iloc[:, 1:]

    # emociones
    columns_p1 = [c for c in p1_homologada.columns if re.search('p\d',c)]   

    def fn(x):
        emotion = x.iloc[1]
        if len(emotion) > 1:
            if x.iloc[2] == 'nr':
                x.iloc[2] = emotion
                x.iloc[1] = ['NR']
            else:
                is_emotion = [e for e in emotion if e in emo_verified['name'].values]
                if is_emotion:
                    x.iloc[1] = is_emotion
                else:
                    x.iloc[1] = ['NR']
        return x 

    def run(col):
        single_p1 = p1_homologada[(p1_homologada['p1_{}_a'.format(col)]!='nr')][['id', 'p1_{}_a'.format(col), 'p1_{}_b'.format(col)]]
        single_p1 = single_p1.dropna()
        single_p1['p1_{}_a'.format(col)] = tt.tokenize(single_p1['p1_{}_a'.format(col)])

        single_p1['p1_{}_a'.format(col)] = tt.check_spelling(single_p1['p1_{}_a'.format(col)])
        single_p1['p1_{}_b'.format(col)] = tt.check_spelling(single_p1['p1_{}_b'.format(col)])

        single_p1 = single_p1.apply(lambda x: fn(x), 1)
        single_p1['p1_{}_a_token'.format(col)] = tt.tokenize(single_p1['p1_{}_b'.format(col)])

        ids_all = [[x[0], xx, x[2], x[3]] for x in single_p1.values for xx in x[1]]

        final = pd.DataFrame(ids_all)
        final.columns = ['ind_id', 'name', 'arg', 'token_arg']
        final['name'] = tt.equivalent_words(final['name'])
        # ONLINE
        single_p1_online = online[['id', 
                                   '{} >> Emociones / Sentimientos / Sensaciones'.format(col),
                                   '{} >> Explique lo mencionado'.format(col)]]
        single_p1_online = single_p1_online.dropna()
        single_p1_online.columns = ['ind_id', 'name', 'arg']
        
        single_p1_online['name'] = tt.tokenize(single_p1_online['name'])
        single_p1_online = single_p1_online.apply(lambda x: fn(x), 1)
        single_p1_online['token_arg'] = tt.tokenize(single_p1_online['arg'])
        single_p1_online['name'] = tt.check_spelling(single_p1_online['name'])

        ids_all = [[x[0], xx, x[2], x[3]] for x in single_p1_online.values for xx in x[1]]
        
        
        final2 = pd.DataFrame(ids_all)
        final2.columns = ['ind_id', 'name', 'arg', 'token_arg']
        final2['name'] = tt.equivalent_words(final2['name'])

        final = pd.concat([final, final2])
        return final

    all_cols = Parallel(n_jobs=num_cores)(delayed(run)(col) \
                        for col in range(1,4)) 
    emotions_df = pd.concat(all_cols)
    emotions_df = emotions_df.sort_values('ind_id').reset_index().iloc[:, 1:]
    emotions_df = emotions_df.replace({'nr': 'NR', '[nr]':'[NR]'})
    
    # necesidades del pais
    def run_need(col):
        single_p2 = consultas[['id', 'p2_{}_a'.format(col), 'p2_{}_b'.format(col), 'p2_urg'.format(col)]]

        # Online inclusion
        single_p2_o = online[['id', 
                              '{} >> Necesidad que enfrenta el país'.format(col), 
                              '{} >> Explique lo mencionado.1'.format(col), 
                              '{} >> Urgencia (solo una)'.format(col)]]
        single_p2_o.columns = ['id', 'p2_{}_a'.format(col), 'p2_{}_b'.format(col), 'p2_urg']

        single_p2 = pd.concat([single_p2, single_p2_o])

        # Continue
        single_p2 = single_p2.dropna()
        single_p2 = single_p2[single_p2['p2_{}_a'.format(col)] != 'nr']
        single_p2['token_need'] = tt.tokenize(single_p2['p2_{}_a'.format(col)])
        single_p2['token_arg'] = tt.tokenize(single_p2['p2_{}_b'.format(col)])
        single_p2 = single_p2.rename(columns={'id': 'ind_id', 'p2_{}_a'.format(col):'need', 'p2_{}_b'.format(col): 'arg',
                                             'p2_urg':'priority'})
        return single_p2

    all_cols = Parallel(n_jobs=num_cores)(delayed(run_need)(col) \
                        for col in range(1,4)) 
    country_need_df = pd.concat(all_cols)
    country_need_df = country_need_df.sort_values('ind_id').reset_index().iloc[:, 1:]
    country_need_df['priority'] = country_need_df['priority'].replace(priority_dic)

    # necesidades personales/familiares
    def run_personal_need(col):
        single_p3 = consultas[['id', 'p3_{}_a'.format(col), 'p3_{}_b'.format(col)]]
        # Online inclusion
        single_p3_o = online[['id', 
                              '{} >> Necesidades que enfrento personalmente o que existen en mi hogar o familia'.format(col), 
                              '{} >> Explique lo mencionado.2'.format(col)]]
        single_p3_o.columns = ['id', 'p3_{}_a'.format(col), 'p3_{}_b'.format(col)]
        single_p3 = pd.concat([single_p3, single_p3_o])

        single_p3 = single_p3.dropna()
        single_p3 = single_p3[single_p3['p3_{}_a'.format(col)] != 'nr']
        single_p3['token_need'] = tt.tokenize(single_p3['p3_{}_a'.format(col)])
        single_p3['token_arg'] = tt.tokenize(single_p3['p3_{}_b'.format(col)])
        single_p3 = single_p3.rename(columns={'id': 'ind_id', 'p3_{}_a'.format(col):'need', 'p3_{}_b'.format(col): 'arg'})
        return single_p3

    all_cols = Parallel(n_jobs=num_cores)(delayed(run_personal_need)(col) \
                        for col in range(1,4)) 
    personal_need_df = pd.concat(all_cols)
    personal_need_df = personal_need_df.sort_values('ind_id').reset_index().iloc[:, 1:]


    # necesidades y rol del estado
    p4_orden_cuestionario = pd.merge(p4_orden_cuestionario, consultas[['id', 'correlativo_digitación']], on=['correlativo_digitación'])

    def run_need_and_rol(col):
        single_p4 = p4_orden_cuestionario[['id_y', 'p4_n{}'.format(col), 'p4_re_{}'.format(col), 'p4_oa_{}'.format(col), 'p4_roa_{}'.format(col)]]
        # Online inclusion
        single_p4_o = online[['id', 
                              '{} >> Necesidades del país identificadas'.format(col), 
                              '{} >> Rol del Estado (Describa)'.format(col),
                              '{} >> Actor social (empresa, organizaciones sociales, medios de comunicación, comunidad, etc)'.format(col),
                              '{} >> Rol del actor social (Describa)'.format(col)]]

        single_p4_o.columns = ['id_y', 'p4_n{}'.format(col), 'p4_re_{}'.format(col), 'p4_oa_{}'.format(col), 'p4_roa_{}'.format(col)]
        single_p4_o = single_p4_o.dropna()
        single_p4 = pd.concat([single_p4, single_p4_o])
        # Continue
        single_p4 = single_p4[single_p4['p4_n{}'.format(col)] != 'nr']

        single_p4['token_need'] = tt.tokenize(single_p4['p4_n{}'.format(col)])
        single_p4['token_re'] = tt.tokenize(single_p4['p4_re_{}'.format(col)])
        single_p4['token_oa'] = tt.tokenize(single_p4['p4_oa_{}'.format(col)])
        single_p4['token_roa'] = tt.tokenize(single_p4['p4_roa_{}'.format(col)])
        single_p4 = single_p4.rename(columns={'id_y': 'ind_id', 'p4_n{}'.format(col):'need', 'p4_re_{}'.format(col): 're',
                                               'p4_oa_{}'.format(col):'oa', 'p4_roa_{}'.format(col):'roa'})
        return single_p4

    all_cols = Parallel(n_jobs=num_cores)(delayed(run_need_and_rol)(col) \
                    for col in range(1,4)) 
    rol_estado_need_df = pd.concat(all_cols)
    rol_estado_need_df = rol_estado_need_df.sort_values('ind_id').reset_index().iloc[:, 1:]

    # contribucion
    contributions = consultas[['id', 'p5']]
    contributions_o = online[['id', '5. Pregunta: ¿Cuál es mi contribución personal para construir el Chile que queremos?']]
    contributions_o.columns = ['id', 'p5']
    contributions = pd.concat([contributions, contributions_o])

    contributions['token'] = tt.tokenize(contributions['p5'])
    contributions = contributions[contributions['p5']!='nr']
    contributions = contributions.dropna()
    contributions = contributions.rename(columns={'id': 'ind_id', 'p5':'text'})

    info_concat.to_csv('./nuevos_datos/individuals/individuals.csv', index=False)
    emotions_df.to_csv('./nuevos_datos/individuals/emotions.csv', index=False)
    country_need_df.to_csv('./nuevos_datos/individuals/country_need.csv', index=False)
    personal_need_df.to_csv('./nuevos_datos/individuals/personal_need.csv', index=False)
    rol_estado_need_df.to_csv('./nuevos_datos/individuals/state_rol.csv', index=False)
    contributions.to_csv('./nuevos_datos/individuals/contributions.csv', index=False)