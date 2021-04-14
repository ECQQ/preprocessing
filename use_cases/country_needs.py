import multiprocessing
import use_cases.utils.textools as tt
import pandas as pd
import numpy as np
import re, os

from joblib import Parallel, delayed


def check_priority_in_text(need, priority):
    needs = [need]
    priorities = []
    # Clean Needs
    if re.search(r'\d+', need):
        cases = re.findall(r'\d+', need)
        # Asumming kind of enumeration
        if len(cases) > 1:
            cases = [int(c) for c in cases]
            delta = np.diff(cases)
            priorities = cases
            # consecutive cases should be 1-spaced located
            if np.mean(delta) == 1:
                splits = re.split(r'\d', need)
                splits = [re.findall('[A-z]+', w) for w in splits]
                needs  = [s[0] for s in splits if len(s) != 0]

        # Identifying priorities in the name
        if len(cases) == 1:
            # If there is a number before the EOS token or a point
            pseudo_prior = re.search(r'\s\d($|\.)', need)
            needs = [re.search(r'\w+', need).group().strip()]

            if pseudo_prior:
                pseudo_prior = pseudo_prior.group()
                pseudo_prior = int(pseudo_prior.strip())
                # if there is not priority
                if np.isnan(priority):
                    priority = pseudo_prior
            priorities.append(priority)

    if priorities == []:
        priorities = [priority]


    return needs, priorities

def clean_string(text):
    text = tt.to_unicode(text)
    text = " ".join(text.split())
    f = re.sub(r'[^a-zA-Z0-9]', ' ', text)
    f = re.sub(' +', ' ',f.strip())
    return f

def get_roles(role_frame, fide, need, exp, prior, who='estado'):

    exp = clean_string(exp)
    role_frame = clean_string(role_frame)

    tuples = []
    if '-' in role_frame:
        roles_frame = role_frame.split('-')
        for rol in roles_frame:
            tuples.append([fide, need, who, rol, exp, prior])
    else:
        tuples.append([fide, need, who, role_frame, exp, prior])

    return tuples


def process_one(row, k):
    re_0 = r'P4'

    frame_cols  = [x for x in row.index if re.search(re_0, x)]

    text_response = []
    token_response = []
    final = None


    #iterate over the rows
    tuples = []

    for k in range(1, 6):
        fide       = row['ID Archivo']


        need_p2    = row['P2_{}_A'.format(k)]
        explan_p2  = row['P2_{}_B'.format(k)]
        priority   = row['P2_{}_C'.format(k)]

        need       = row['P4_{}_A'.format(k)]
        rol_estado = row['P4_{}_B'.format(k)]
        other_act  = row['P4_{}_C'.format(k)] # other actor
        rol_otro   = row['P4_{}_D'.format(k)] # role of the other actor

        # Other actors
        other_act = tt.to_unicode(other_act)

        other_act = re.sub(r'\W+', ' ', other_act)
        if 'respuesta sin completar' in other_act \
            or pd.isna(other_act) \
            or other_act == 'nan':
            other_act = 'NR'

        # to unicode
        need_p2 = tt.to_unicode(need_p2)
        need_p2 = " ".join(need_p2.split())

        need = tt.to_unicode(need)
        need = " ".join(need.split())


        needs, priorities = check_priority_in_text(need, priority)

        for n, p in zip(needs, priorities):

            if n == need_p2:
                state_tuple = get_roles(rol_estado, fide, n, explan_p2, p, who='estado')
                tuples+=state_tuple
                if other_act != 'NR':
                    other_tuple = get_roles(rol_otro, fide, n, explan_p2, p, who=other_act)
                    tuples+=other_tuple
            else:
                state_tuple = get_roles(rol_estado, fide, n, 'NR', k, who='estado')
                tuples+=state_tuple

                if other_act != 'NR':
                    other_tuple = get_roles(rol_otro, fide, n, 'NR', k, who=other_act)
                    tuples+=other_tuple

                if need_p2.lower().strip() != 'nan':
                    other_tuple = get_roles('NR', fide, need_p2, explan_p2, p, who='estado')
                    tuples+=other_tuple

    df = pd.DataFrame(np.array(tuples),
            columns=['file_id', 'name', 'actor', 'role', 'explanation', 'priority'])
    df = df[df['name'] != 'nan']
    df = df[~df['name'].isna()]
    df['name'] = tt.check_spelling(df['name'])
    df['actor'] = tt.equivalent_words(df['actor'])
    df['role_token'] = tt.tokenize(df['role'])
    df['exp_token'] = tt.tokenize(df['explanation'])
    df['priority'] = df['priority']
    df = df.replace({'nan':'NR'})
    df =df.sort_values('priority')

    df['priority'] = df['priority'].astype(int)

    if df.shape[0] != 0:
        if df['priority'].values[0] == 0:
            df['priority'] = df['priority'].apply(lambda x: x+1)
    return df

def create_table_country_needs(frame):
    # Init Pipeline
    num_cores = multiprocessing.cpu_count()

    for k in range(1, 6):
        fide       = frame['ID Archivo']

        need_0 = frame['P2_{}_A'.format(k)]
        exp    = frame['P2_{}_B'.format(k)]
        rank   = frame['P2_{}_C'.format(k)]

        need_1 = frame['P4_{}_A'.format(k)]
        state_role  = frame['P4_{}_B'.format(k)]
        other_actor = frame['P4_{}_C'.format(k)] # other actor
        actor_role  = frame['P4_{}_D'.format(k)] # role of the other actor

        need_0 = tt.to_unicode(need_0)
        print(need_0)

        break

    # Run script for each froup
    # allframes = Parallel(n_jobs=num_cores)(delayed(process_one)(row, k) \
    #                 for k, row in frame.iterrows())

    # needs = pd.concat(allframes)
    # needs['need_id'] = range(0, needs.shape[0])
    # needs['macro'] = needs['name']
    # needs['role_token'] = tt.equivalent_words(needs['role_token'])

    # return needs
