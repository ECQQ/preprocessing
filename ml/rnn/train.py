import tensorflow as tf 

from core.data import load_records
from core.model import RNNModel


# batches = load_records('./data/records/contributions/train/', batch_size=10)


batch_size = 10
model = RNNModel(num_units=16, num_layers=2, num_cls=20)

model.model(batch_size).summary()