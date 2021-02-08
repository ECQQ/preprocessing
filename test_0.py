import unittest 
import pandas as pd
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

if __name__ == '__main__':
    unittest.main()