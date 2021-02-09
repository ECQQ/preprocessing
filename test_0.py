import unittest 
import pandas as pd
import numpy as np
import textools 
import warnings

warnings.filterwarnings("ignore")

class TestPreprocessing(unittest.TestCase):
    
    def test_tokenize(self):
        source = 'datos/Dialogo/BBDD_dialogos_final.csv'
        chunksize = 50
        data = pd.read_csv(source, chunksize=chunksize)    
        for frame in data: break
        
        column_0 = frame['P1_1_A']
        column_1 = textools.tokenize(column_0)

        self.assertTrue(type(column_0)==type(column_1),
                        'FOO') 

    def test_separate_words(self):
        source = 'datos/Dialogo/BBDD_dialogos_final.csv'
        chunksize = 50
        data = pd.read_csv(source, chunksize=chunksize)    
        for frame_0 in data: break
        
        column_0 = frame_0['P1_1_A']
        column_1 = textools.tokenize(column_0)

        frame_1 = textools.replace_col(frame_0, column_1)
        self.assertTrue(frame_0.shape[0]<frame_1.shape[0],
                        'FOO') 

    def test_spelling(self):
        source = 'datos/Dialogo/BBDD_dialogos_final.csv'
        chunksize = 50
        data = pd.read_csv(source, chunksize=chunksize)    
        for frame_0 in data: break

        column_0 = frame_0['P1_1_A']
        column_1 = textools.tokenize(column_0)

        frame_1 = textools.replace_col(frame_0, column_1)
        
        column_2 = textools.check_spelling(frame_1['P1_1_A'])
        column_3 = textools.equivalent_words(column_2)

        self.assertTrue(column_3[20] == 'impotencia','FOO')  
    
    def test_nan_values(self):
        source = 'datos/Dialogo/BBDD_dialogos_final.csv'
        chunksize = 100
        data = pd.read_csv(source, chunksize=chunksize)    
        for frame_0 in data: break
        frame_0 = frame_0[['P1_1_A', 'P1_3_B']]
        frame_1 = textools.remove_nans(frame_0)
        u = np.unique(frame_1.isna().values)
        self.assertTrue(True not in u)

        col_0 = frame_0['P1_3_B']
        col_1 = textools.remove_nans(col_0)

    def test_acronyms(self):
        source = 'datos/Dialogo/BBDD_dialogos_final.csv'
        chunksize = 10
        data = pd.read_csv(source, chunksize=chunksize)    
        for frame_0 in data: break
        frame_0 = frame_0['P1_1_A']

        frame_0.iloc[2] = 'enojado con las afp'
        frame_0.iloc[3] = 'enojado con las A.F.P'
        frame_0.iloc[4] = 'enojado con las AF.P'
        frame_0.iloc[5] = 'enojado con las AFP'
        frame_0.iloc[6] = 'molesto, con. las FF.aA,'
        frame_0.iloc[7] = 'PSU qla la iodio!'
        r = textools.short_words(frame_0)
        self.assertTrue(r == ['AFP', 'AFP', 'AFP', 'AFP', 'FFAA', 'PSU'])

if __name__ == '__main__':
    unittest.main()