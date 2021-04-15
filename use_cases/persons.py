import use_cases.utils.textools as tt
import pandas as pd
import numpy as np
import re, os

from use_cases.utils.formatter import (regiones_name,
									   regiones_iso,
				                       sex_wrong,
				                       education_options,
				                       education_wrong,
				                       education_name)
									   
from use_cases.utils.comunas import get_comunas_id



def fix_swapped(col, i):
	if col['LP_COD_SEXO_P{}'.format(i)] in sex_wrong:
		aux = col['LP_COD_SEXO_P{}'.format(i)]
		col['LP_COD_SEXO_P{}'.format(i)] = col['LP_COD_NIVEL_P{}'.format(i)]
		col['LP_COD_NIVEL_P{}'.format(i)] = aux.replace('_', ' ')

	if col['LP_COD_NIVEL_P{}'.format(i)] in education_wrong:
		aux = col['LP_COD_NIVEL_P{}'.format(i)]
		col['LP_COD_NIVEL_P{}'.format(i)] = col['LP_COD_SEXO_P{}'.format(i)].replace('_', ' ')
		col['LP_COD_SEXO_P{}'.format(i)] = aux

	return col

def distributed(frame, i):
	single = frame[['ID Archivo',
	                'LP_EDAD{}'.format(i),
					'LP_COD_SEXO_P{}'.format(i),
					'LP_COD_NIVEL_P{}'.format(i),
					'LP_COMUNA{}'.format(i)]]

	single = tt.to_unicode(single)
	single = single.apply(lambda x: fix_swapped(x, i), 1)
	single.columns = ['file_id', 'age', 'sex', 'level', 'comuna_id']

	single = single.apply(lambda x: get_comunas_id(x, 'comuna_id'), 1)
	single = single[~single['age'].isna()]
	single = single[single['sex'] != 'nan']
	single['age'] = single['age'].apply(lambda x: x[:2])
	single['age'] = single['age'].astype(int)
	single = tt.stratify_frame_by_age(single)
	single = single.replace({0:'', '0':'', 'nr':''})
	return single


def create_table(frame):
	max_member = 30
	frame['Grupo'] = tt.check_nan(frame['Grupo'])

	table_cols = []
	for i in range(1, max_member+1):
		 table_cols.append(distributed(frame, i))
	table = pd.concat(table_cols)

	return table
