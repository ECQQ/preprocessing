import tensorflow as tf 

from core.data import load_records

batches = load_records('./data/records/contributions/train/', batch_size=10)