import pandas as pd
import numpy as np
import nltk
import re

from nltk.corpus import stopwords
from autocorrect import Speller
from unidecode import unidecode
from sklearn.neighbors import KDTree


nltk.download('stopwords')

def tokenize(column):
    """ Tokenize a given column
    Args:
        column ([Serie]): a pandas column
    Return:
        a pandas column with tockens lists
    """
    def step(cell):
        # Remove special characters
        cell = unidecode(str(cell))
        # Lowercase
        cell = cell.lower()
        # Extract only words
        rfilter = r'[a-z]+'
        finds = re.findall(rfilter, cell)
        # Remove one-letters words 
        finds = [f for f in finds if len(f)>1]
        # Remove stop words
        finds = [f for f in finds \
        if f not in stopwords.words('spanish')]

        return finds

    assert isinstance(column, pd.Series), \
    'Column should be a pandas Serie. {} was received instead'.format(type(column))
    
    column = column.apply(lambda x: step(x))
    return column

def replace_col(frame, column):
    """Replace a column in a frame
    Args:
        frame ([pandas Dataframe]): [pandas dataframe]
        column ([pandas Series]): [new column]
    """
    col_name = column.name
    col_indices = column.index.values

    new_frame = [frame.iloc[i]
                   for i in col_indices\
                   for v in column.iloc[i]]
    new_frame = pd.concat(new_frame, 1)
    new_frame = new_frame.transpose()

    new_frame[col_name] = [vv for v in column for vv in v]
    
    return new_frame

def check_spelling(column):
    """ Corrects col spelling automatically.
    Right now this function only works on words 
    (no sentences)
    Args:
        column ([Series]): [a frame column]

    Returns:
        [type]: [description]
    """
    spell = Speller(lang='es')
    
    for i, cell in enumerate(column):
        if ' ' in cell:
            # by sentence case
            pass
        else:
            # by word case
            corr = spell(cell)
            column.iloc[i] = corr     
    return column

def equivalent_words(column):
    """ Replace words by similarity.
    We calculate similarity by setting letter weights.
    This function works only on words (no sentences)
    Args:
        column ([Serie]): [a pandas column]

    Returns:
        [Serie]: [the same column with similar words changed]
    """
    values = column.values

    def get_score(word):
        scores = [ord(letter) for letter in word]
        return sum(scores)

    scores = column.apply(lambda x: get_score(x))
    
    tree = KDTree(scores)
    dist, ind = tree.query(scores, k=2)
    print(dist)  

    return column