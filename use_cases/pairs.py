import pandas as pd

def create_pair_token(frame, col_tokens, id_source_name):  
    rows = []
    pair_id = 1

    for index, row in frame.iterrows():
        tokens = row[col_tokens]
        for i in range(0,len(tokens) - 1):
            new_row = {
                'id' : pair_id,
                id_source_name : row['id'],
                'word_1' : tokens[i],
                'word_2' : tokens[i+1]
            }
            rows.append(new_row)
            pair_id += 1

    pairs = pd.DataFrame(rows, columns = ['id', id_source_name ,'word_1', 'word_2'])
    return pairs 
