import use_cases.utils.textools as tt
from use_cases.utils.comunas import get_comunas_id
import pandas as pd
import numpy as np
import re, os


def create_table_dialogues(frame):
    max_member = 30
    frame['Grupo'] = tt.check_nan(frame['Grupo'])

    frame = frame[['ID Archivo', 'Fecha', 'Hora Inicio',
                    'Hora Termino', 'Lugar', 'Dirección',
                    'Comuna', 'Participantes',
                    'Grupo']]


    frame = tt.to_unicode(frame)
    frame = frame.replace({0:'', 'nan':'', 'nr':''})

    frame.columns =['file_id', 'date', 'init_time', 'end_time',
                    'location', 'address', 'comuna_id', 'n_members',
                    'group_name']

    frame = frame.apply(lambda x: get_comunas_id(x, 'comuna_id'), 1)                

    return frame
