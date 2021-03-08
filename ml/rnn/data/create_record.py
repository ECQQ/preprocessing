 
import multiprocessing as mp
import tensorflow as tf 
import pandas as pd
import numpy as np
import os

from joblib import Parallel, delayed
from transformers import XLMTokenizer
from tqdm import tqdm


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

    return (train, 'train'), (vali, 'val'), (test, 'test') 

def process_sentence(text, label, dicmap):
    ex = None
    if len(text) > 2:
        tokenizer = XLMTokenizer.from_pretrained("xlm-mlm-100-1280")
        input_ids = tokenizer.encode(text)

        f = dict()
        f['text'] = _bytes_feature(text.encode('utf-8'))
        f['input'] = _float_feature(input_ids)
        f['label'] = _int64_feature(dicmap.index(label))
        # f['length'] = _int64_feature(len(input_ids))
        f['category'] = _bytes_feature(label.encode('utf-8'))
        ex = tf.train.Example(features=tf.train.Features(feature=f))
    return ex

def write_record(subset, folder, filename):
    # Creates a record file for a given subset of lightcurves
    os.makedirs(folder, exist_ok=True)
    with tf.io.TFRecordWriter('{}/{}.record'.format(folder, filename)) as writer:
        for ex in subset:
            writer.write(ex.SerializeToString())

def create_record(file, path='./records/', val_ptge=0.25, test_ptge=0.25, n_jobs=None):  
    n_jobs = mp.cpu_count() if n_jobs is not None else n_jobs

    # Reading data frame
    data = pd.read_csv(file)
    data = data[data['resume']!='NR']
    unique_classes = list(data['resume'].unique())

    # Group by category
    grp_class = data.groupby('resume')

    # Iterate over sentences
    for label, lab_frame in tqdm(grp_class):
        response = Parallel(n_jobs=n_jobs)(delayed(process_sentence)\
                    (text, label, unique_classes) \
                    for text in lab_frame['text'])

        response = [r for r in response if r is not None]
        splits = train_val_test_split(response, 
                                      val_ptge=val_ptge, 
                                      test_ptge=test_ptge)
        Parallel(n_jobs=n_jobs)(delayed(write_record)\
                (subset, '{}/{}'.format(path, name), unique_classes.index(label)) \
                for subset, name in splits)



frame_file = './data/contributions.csv'
folder_destinity = './data/records/contributions'
create_record(frame_file, path=folder_destinity)