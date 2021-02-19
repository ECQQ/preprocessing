import unittest 
import pandas as pd
import numpy as np
import warnings
import re 
import sys,os
sys.path.append(os.path.realpath('..'))
import textools 

from use_cases import requirements as script
warnings.filterwarnings("ignore")

class TestPreprocessing(unittest.TestCase):
    
    def test_tokenize(self):
        source = '../datos/Dialogo/BBDD_dialogos_final.csv'
        chunksize = 50
        data = pd.read_csv(source, chunksize=chunksize)    
        for frame in data: break
    

        column_0 = frame['P5_1']
        column_1 = textools.tokenize(column_0)

        self.assertTrue(type(column_0)==type(column_1),
                        'FOO') 

    def test_combine_list_of_words(self):
        source = '../datos/Dialogo/BBDD_dialogos_final.csv'
        chunksize = 50
        data = pd.read_csv(source, chunksize=chunksize)    
        for frame in data: break
        # self.assertTrue(column_3[20] == 'impotencia','FOO')
  
    def test_pipeline(self):
        source = '../datos/Dialogo/BBDD_dialogos_final.csv'
        chunksize = 50
        data = pd.read_csv(source, chunksize=chunksize)    
        for frame in data: break

        for k, row in frame.iterrows():
            df = script.process_one(row, k) 
            print(df)
            break

if __name__ == '__main__':
    unittest.main()