import multiprocessing
import textools as tt
import pandas as pd
import numpy as np
import re, os

from joblib import Parallel, delayed
from .formatter import (regiones_name, 
						regiones_iso,
                       	sex_wrong,
                       	education_options, 
                       	education_wrong, 
                       	education_name)


def process_one(row, comunas_df, k):
	info = []
	for i in range(1, max_member+1):
		age     = tt.check_nan(row['LP_EDAD{}'.format(i)])
		sex     = tt.check_nan(row['LP_COD_SEXO_P{}'.format(i)])
		level   = tt.check_nan(tt.to_unicode(row['LP_COD_NIVEL_P{}'.format(i)]))
		if sex in sex_wrong:
			aux = sex
			sex = level.capitalize()
			level = aux.replace('_', ' ')

		if level not in education_options:
			if level in education_wrong:
				aux = sex
				sex = level.capitalize()
				level = aux.replace('_', ' ')
		if level != 'NR':
			if level in education_name.keys():
				level = education_name[level]

		comuna  = tt.check_nan(row['LP_COMUNA{}'.format(i)])
		region  = tt.check_nan(comunas_df[comunas_df['Comuna'] == tt.to_unicode(comuna)]['Region'])
		if region == 'NR':
			region_name = 'NR'
			region_iso = 'NR'
		else:
			region_code = int(region) - 1
			region_name = regiones_name[region_code]
			region_iso = regiones_iso[region_code]

		file_id = tt.check_nan(row['ID Archivo'])
		info.append([file_id, age, sex, level, comuna, region, region_name, region_iso])

	df = pd.DataFrame(info)
	df.columns = ['file_id', 'age', 'sex', 'level', 'comuna', 'num_region', 'region', 'region_iso']
	df = df[df['age'] != 'NR']
	df['age'] = df['age'].astype(int)
	df = tt.stratify_frame_by_age(df)

	return df

def get_dialogue_members(frame):
	selected = frame[['ID Archivo', 'Fecha', 'Hora Inicio', 
	                  'Hora Termino', 'Lugar', 'Dirección', 
	                  'Comuna', 'Region', 'Participantes', 
	                  'Grupo']]
	selected['Grupo'] = tt.check_nan(selected['Grupo'])           
	comunas_df = pd.read_csv('./datos/comuna_region.csv')

	num_cores = multiprocessing.cpu_count()
	# Run script for each froup
	allframes = Parallel(n_jobs=num_cores)(delayed(process_one)(row, comunas_df, k) for k, row in selected.iterrows())

	print(allframes)