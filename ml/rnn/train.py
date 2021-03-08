import tensorflow as tf 
import os 

from core.data import load_records
from core.model import RNNModel

from tensorflow.keras.losses import CategoricalCrossentropy
from tensorflow.keras.metrics import Recall, CategoricalAccuracy

root = './data/records/contributions/'
train_batches, n_cls = load_records(os.path.join(root, 'train'), batch_size=10, return_cls=True)
val_batches = load_records(os.path.join(root, 'val'), batch_size=10)

batch_size = 10
model = RNNModel(num_units=16, num_layers=2, num_cls=n_cls)

model.model(batch_size).summary()

model.compile(optimizer="adam", 
              loss=CategoricalCrossentropy(), 
              metrics=[Recall(), CategoricalAccuracy()])

history = model.fit(train_batches,
                    epochs=20,
                    # callbacks=callbacks,
                    validation_data=val_batches,
                    verbose=1)