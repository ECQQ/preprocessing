
import multiprocessing as mp
import tensorflow as tf
import pandas as pd
import numpy as np
import os, re

from gensim.models.wrappers import FastText
from gensim import corpora, models
from joblib import Parallel, delayed
# from transformers import XLMTokenizer
from tqdm import tqdm

from nltk import word_tokenize
from nltk.stem import SnowballStemmer
stemmer = SnowballStemmer('spanish')

# tokenizer = XLMTokenizer.from_pretrained("xlm-mlm-100-1280")
wordvector = FastText.load_fasttext_format('./data/fasttext-sbwc.bin')

frame_file = './data/contributions.csv'

def clean_alt_list(list_):
    list_ = list_.replace('[', '')
    list_ = list_.replace(']', '')
    list_ = list_.replace("'", '')
    list_ = list_.split(',')

    return list_

exchange = {'Participacion':'Participacion',
            'Participación Ciudadana':'Participacion',
            'Familia':'Reciprocidad-Redes',
            'Participación Electoral':'Participacion',
            'Protesta Social':'Protesta Social',
            'Reciprocidad-Redes':'Reciprocidad-Redes',
            'Sustentabilidad Ambiental':'Sustentabilidad Ambiental',
            'Compromiso con la Educación y Autoeducación':'Educación y autoeducación',
            'Difusión de la Información':'Educación y autoeducación',
            'Voluntariado':'Voluntariado',
            'Cultura':'Cultura',
            'Trabajo':'Trabajo',
            'Accountability':'Confianza en las instituciones',
            'Combatir Delincuencia':'Combatir Delincuencia',
            'Defensa de derechos':'Defensa de derechos',
            'Defensa de derechos, Inclusión y Diversidad':'Inclusión y Diversidad',
            'Inclusión y Diversidad':'Inclusión y Diversidad',
            'Apoyo a Pueblos Originarios':'Inclusión y Diversidad',
            'Emprendimiento':'Trabajo',
            'Autocuidado y Salud':'Autocuidado y Salud',
            'Erradicar violencia contra la Mujer':'Erradicar violencia contra la Mujer',
            'Tenencia Responsable':'Sustentabilidad Ambiental',
            'Confiar en la Institucionalidad':'Confianza en las instituciones'}

# Reading data frame
data = pd.read_csv(frame_file)
data['tokens'] = data['tokens'].apply(clean_alt_list)
data = data[data['resume']!='NR']

# data['resume'] = data['resume'].apply(lambda x: exchange[x])

unique_classes = list(data['resume'].unique())

unique_words = list(np.unique([stemmer.stem(word) for tokens in data['tokens'] for word in tokens]))

print('[VOCABSIZE]', len(unique_words))

def _bytes_feature(value):
    """Returns a bytes_list from a string / byte."""
    if isinstance(value, type(tf.constant(0))):
        value = value.numpy() # BytesList won't unpack a string from an EagerTensor.
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))

def _float_feature(list_of_floats):  # float32
    return tf.train.Feature(float_list=tf.train.FloatList(value=list_of_floats))

def _int64_feature(value):
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

def train_val_test_split(x, test_ptge=0.25, val_ptge=0.25):
    size = len(x)
    indices = np.arange(0, size)
    np.random.shuffle(indices)

    test = x[: int(size*test_ptge)]
    vali = x[int(size*test_ptge):int(size*test_ptge) + int(size*val_ptge)]
    train = x[int(size*test_ptge) + int(size*val_ptge):]
    print(len(train))
    return (train, 'train'), (vali, 'val'), (test, 'test')

def write_record(subset, folder, filename):
    # Creates a record file for a given subset of lightcurves
    os.makedirs(folder, exist_ok=True)
    with tf.io.TFRecordWriter('{}/{}.record'.format(folder, filename)) as writer:
        for ex in subset:
            writer.write(ex.SerializeToString())

def process_sentence_fasttext(row, label, dicmap=None):
    ex = None
    text = str(row['text'])
    s = len(row['text'])
    try:
        sentence_words = [re.search(r'[\w \d]+',t.lower().strip()) for t in text.split()]
        sentence_words = [sw[0].replace('_?','[sub]').replace('_','') for sw in sentence_words if sw is not None]
        input_ids = [wordvector[w] for w in sentence_words]
    except Exception as e:
        input_ids = []
        for t in sentence_words:
            if re.search(r'\d+', t):
                if re.split('\d+',t)[-1] != '':
                    input_ids.append(wordvector[re.split('\d+',t)[-1]])
                input_ids.append(wordvector['[num]'])

            elif re.search(r'[\w]+', t):
                input_ids.append(wordvector[t])

    if len(input_ids) > 0:
        f = dict()
        f['text'] = _bytes_feature(text.encode('utf-8'))
        for k, dim_tok in enumerate(np.array(input_ids)):
            f['dim_tok_{}'.format(k)] = _float_feature(dim_tok)
        f['label'] = _int64_feature(dicmap.index(label))
        f['length'] = _int64_feature(len(input_ids))
        f['category'] = _bytes_feature(label.encode('utf-8'))
        ex = tf.train.Example(features=tf.train.Features(feature=f))
    return ex

def create_record(path='./records/', val_ptge=0.25, test_ptge=0.25, n_jobs=None):
    n_jobs = mp.cpu_count() if n_jobs is not None else n_jobs

    # Group by category
    grp_class = data.groupby('resume')

    # Iterate over sentences
    for label, lab_frame in tqdm(grp_class):
        response = Parallel(n_jobs=n_jobs)(delayed(process_sentence_fasttext)\
                    (row, label, unique_classes) \
                    for k, row in lab_frame.iterrows())

        response = [r for r in response if r is not None]
        splits = train_val_test_split(response,
                                      val_ptge=val_ptge,
                                      test_ptge=test_ptge)
        Parallel(n_jobs=n_jobs)(delayed(write_record)\
                (subset, '{}/{}'.format(path, name), unique_classes.index(label)) \
                for subset, name in splits)

folder_destinity = './data/records/all_contrib'
create_record(path=folder_destinity, val_ptge=0.25, test_ptge=0.25)
