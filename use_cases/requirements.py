import textools as tt
import pandas as pd
import re


def process_one(row, k):
    re_0 = r'P5_\d'
    re_1 = r'LP_EDAD'

    frame_cols  = [x for x in row.index if re.search(re_0, x)]
    member_cols = [x for x in row.index if re.search(re_1, x)]
    finals_df   = []
    for m in member_cols:
        age = row[m]
        if not pd.isna(age):
            text_response = []
            token_response = []
            for column in frame_cols: 
                
                single_col = pd.Series(row[column])
                single_col = tt.tokenize(single_col)
                single_col = single_col.values[0]
                if 'nan' not in single_col:
                    token_response.append(single_col)
                    text_response.append(row[column])

            # TEXT RESPONSE CLEANING
            text_response = pd.Series(text_response)
            text_response = tt.check_spelling(text_response)
            text_response = tt.equivalent_words(text_response)
            # TOKEN BASED RESPONSE CLEANING
            token_response = pd.Series(token_response)
            token_response = tt.check_spelling(token_response)
            token_response = tt.equivalent_words(token_response) 
            # FILTERING NAN VALUES
            req_serie_text  = text_response[(~pd.isna(text_response)) & (text_response != 'nan')]
            req_serie_token = token_response[(~pd.isna(token_response)) & (token_response != 'nan')]
            # GETTING IDS FOR ADDING TO OLD DATAGRTAMES
            user_id   = re.search(r'\d+', m).group() 
            id_file   = row['ID Archivo'] 
            # AT LEAST ONE ANSWER
            if req_serie_token.shape[0] > 1:
                final = []
                for colp in [user_id, id_file]:
                    serie = pd.Series([colp]*len(req_serie_token))
                    final.append(serie)
                final.append(req_serie_text)
                final.append(req_serie_token)
                final = pd.concat(final, 1)
                final.columns = ['id_user', 'id_file', 'req_text' , 'req_token']
                finals_df.append(final)
    
    if len(finals_df)>=1:
        finals_df = pd.concat(finals_df)
        return finals_df
    else:
        return None