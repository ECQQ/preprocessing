import multiprocessing
import use_cases.utils.textools as tt
import pandas as pd
import numpy as np
import re, os

from joblib import Parallel, delayed


def check_priority_in_text(row):
    need = row['name']
    priority = row['priority']

    needs = [need]
    priorities = []
    # Clean Needs
    if re.search(r'\d+', need):
        cases = re.findall(r'\d+', need)
        # Asumming kind of enumeration
        if len(cases) > 1:
            cases = [int(c) for c in cases]
            delta = np.diff(cases)
            priorities = cases+1
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

    row['name'] = needs
    row['priority'] = priorities
    return row

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

def create_table_country_needs(frame):
    # Init Pipeline
    num_cores = multiprocessing.cpu_count()

    frames = []
    for k in range(1, 6):
        need_0 = frame[['ID Archivo',
                        'P2_{}_A'.format(k),
                        'P2_{}_B'.format(k),
                        'P2_{}_C'.format(k)]]
        need_0.columns = ['file_id', 'name', 'exp', 'priority']
        need_1 = frame[['ID Archivo',
                        'P4_{}_A'.format(k),
                        'P4_{}_B'.format(k),
                        'P4_{}_C'.format(k),
                        'P4_{}_D'.format(k)]]
        need_1.columns = ['file_id', 'name', 'state_role', 'actor', 'role_actor']

        need_0 = tt.to_unicode(need_0)
        need_1 = tt.to_unicode(need_1)

        # Check priority
        need_0 = need_0.apply(lambda x: check_priority_in_text(x), 1)
        rows = [(c['file_id'], n, c['exp'], int(c['priority'][0])) for k, c in need_0.iterrows()\
                    for n in c['name']]
        need_0 = pd.DataFrame(rows, columns=['file_id','name', 'exp', 'priority'])

        need_1_other = need_1[need_1['actor'] != 'nan'][['file_id', 'name', 'actor', 'role_actor']]
        need_1_state = need_1[need_1['actor'] == 'nan'][['file_id', 'name', 'state_role']]
        need_1_state['actor'] = ['estado']*need_1_state.shape[0]
        need_1_state.columns = ['file_id', 'name', 'role', 'actor']
        need_1_other.columns = ['file_id', 'name', 'actor', 'role']
        need_1 = pd.concat([need_1_state, need_1_other])

        result = pd.concat([need_0, need_1], axis=1)

        result = pd.merge(need_0, need_1, how="outer", on=["name", "file_id"])

        frames.append(result)


    needs = pd.concat(frames)
    needs['id'] = range(0, needs.shape[0])
    needs['name_tokens'] = tt.tokenize(needs['name'])
    needs['macro'] = ['']*needs.shape[0]
    needs['exp_tokens'] = tt.tokenize(needs['exp'])
    needs['role_tokens'] = tt.tokenize(needs['role'])

    needs = needs[['id', 'name', 'name_tokens', 'macro', 'exp',
                    'exp_tokens', 'role', 'role_tokens', 'actor', 'priority']]

    needs = needs.fillna('')
    needs = needs.replace({'NR':'', 'nan':'', '-':''})

    return needs
