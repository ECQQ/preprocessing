import tensorflow as tf
import os

def _bytes_feature(value):
    """Returns a bytes_list from a string / byte."""
    if isinstance(value, type(tf.constant(0))):
        value = value.numpy() # BytesList won't unpack a string from an EagerTensor.
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))

def _float_feature(list_of_floats):  # float32
    return tf.train.Feature(float_list=tf.train.FloatList(value=list_of_floats))

def _int64_feature(value):
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

def _parse(sample):
    feat_keys = dict() # features for record
    feat_keys['input'] = tf.io.FixedLenSequenceFeature([],
                                                    dtype=tf.float32,
                                                    allow_missing=True)
    feat_keys['text'] = tf.io.FixedLenSequenceFeature([],
                                                    dtype=tf.string,
                                                    allow_missing=True)
    feat_keys['category'] = tf.io.FixedLenSequenceFeature([],
                                                    dtype=tf.string,
                                                    allow_missing=True)
    feat_keys['label'] = tf.io.FixedLenFeature([], tf.int64)

    # feat_keys['length'] = tf.io.FixedLenFeature([], tf.int64)    
    ex1 = tf.io.parse_example(sample, feat_keys)

    return ex1['text'], ex1['label']


def load_records(source, batch_size):

    datasets = [tf.data.TFRecordDataset(os.path.join(source, x)) for x in os.listdir(source)]  

    datasets = [
        dataset.map(
            lambda x: _parse(x), num_parallel_calls=8) for dataset in datasets
    ]
    
    # datasets = [dataset.repeat() for dataset in datasets]
    datasets = [dataset.shuffle(1000, reshuffle_each_iteration=True) for dataset in datasets]
    dataset  = tf.data.experimental.sample_from_datasets(datasets)
    dataset  = dataset.cache()
    dataset  = dataset.padded_batch(batch_size).prefetch(buffer_size=tf.data.experimental.AUTOTUNE)
    return dataset