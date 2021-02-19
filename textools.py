import pandas as pd
import numpy as np
import difflib
import os.path
import pickle
import nltk
import re

from nltk.corpus import stopwords
from autocorrect import Speller
from unidecode import unidecode

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


nltk.download('stopwords')

def to_unicode(column):
    if not isinstance(column, pd.Series):
        column = pd.Series(column)
        column = column.apply(lambda x: unidecode(str(x)).lower())
        return column.values[0]
    else:
        column = column.apply(lambda x: unidecode(str(x)).lower())
        return column 

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
                 for _ in range(len(column.iloc[i]))]

    new_values = [v for i in col_indices for v in column.iloc[i]]

    new_frame = pd.concat(new_frame, 1)
    new_frame = new_frame.transpose()

    new_frame[col_name] = new_values  
    
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
        if isinstance(cell, list):
            corr = []
            for w in cell:
                w_corr = spell(w)
                corr.append(w_corr)
        else:
            # by word case
            corr = spell(cell)
        column.iloc[i] = corr     
    return column

def equivalent_words(column, values=None):
    """ Replace words by similarity.
    We calculate similarity by setting letter weights.
    This function works only on words (no sentences)
    Args:
        column ([Serie]): [a pandas column]

    Returns:
        [Serie]: [the same column with similar words changed]
    """
    values = column.values if values is None else values

    all_values = [vv for v in values for vv in v]
    new_values = []
    for k, v in enumerate(values):
        if isinstance(v, list):
            words = []
            for w in v:
                c = difflib.get_close_matches(w, 
                            all_values, # this should be changed 
                            n=2)
                words.append(c[0])
            column.iloc[k] = words
        else:
            # here we can use Diego's dictonary
            c = difflib.get_close_matches(v, 
                                        values, # this should be changed 
                                        n=2)
            column.iloc[k] = c[-1]

    return column

def remove_nans(frame):
    """ Remove rows with nan values

    Args:
        frame (pandas Series or Dataframe): column(s)

    Returns:
        frame: the same data structure without nans
    """
    frame = frame.dropna()
    return frame 


def get_google_sheet(sheetid, rangex):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheetid,
                                range=rangex).execute()
    values = result.get('values', [])
    df = pd.DataFrame(values[1:], columns=values[0])
    return df 


def short_words(column):
    """ Extract short words such as:
    acronyms, abbreviations...
    To use this method you need credentials
    contact with Cristobal (cridonoso@inf.udec.cl)
    if you need it
    Args:
        column (Serie): a column with words or sentences

    Returns:
        [list]: a list with short words detected
    """
    sheetid = '1vk3CPLCRZbToaqJQA1_4_1KeH9s2Lfi0cCz0hHewh9s'
    masterdic = get_google_sheet(sheetid, 'A:C')
    acronimos = masterdic[masterdic['clase'] == 'sigla']['palabra'].values

    filter = r'[A-z | \.]+'
    acron_detected = []
    for cell in column:
        cell = unidecode(str(cell))
        # Lowercase
        cell = cell.lower()

        finds=re.findall(filter, cell)
        finds = [f.replace('.', '') for f in finds]
        finds = ' '.join(finds)

        finds = [f.upper() for f in finds.split() if f.upper() in acronimos]
        acron_detected += finds
        
    return acron_detected

def combine_versions(frame1, frame2, on='id_user', which=None):

    if which is not None:
        frame2 = frame2[[on]+which]

    print(frame2)
    if frame1.shape[0] != frame2.shape[0]:
        print('[WARNING] Dataframes have not equal size.')

    frame1 = frame1.set_index(on)
    frame2 = frame2.set_index(on)

    result = pd.concat([frame1, frame2], axis=1)

    result.reset_index(level=0, inplace=True)

    return result

def stratify_frame_by_age(frame):
    # Stratify 
    etiquetas = ['{}-{}'.format(n, n+15) for n in range(0, 60, 15)]
    etiquetas += ['>=60']
    range_values = [i for i in range(0, 61, 15)]
    range_values.append(100)
    frame['age_range'] = pd.cut(frame['age'], range_values, right=False, labels=etiquetas)
    return frame

def check_nan(condition, when_nan='NR'):
    try:
        if isinstance(condition, pd.Series):
            response = condition.values[0]
        else:
            response = 'NR' if (condition=='nan' or pd.isna(condition)) else condition
    except Exception as e:
        response = 'NR'
    return response
    