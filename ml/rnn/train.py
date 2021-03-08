import tensorflow as tf 
import argparse
import json
import os 

from core.data import load_records
from core.model import RNNModel
from core.callbacks import get_callbacks

from tensorflow.keras.losses import CategoricalCrossentropy
from tensorflow.keras.metrics import Recall, CategoricalAccuracy
from tensorflow.keras.optimizers import RMSprop, Adam

def train(opt):
    train_batches, n_cls = load_records(os.path.join(opt.data, 'train'), 
                                        batch_size=opt.batch_size, 
                                        return_cls=True)
    val_batches  = load_records(os.path.join(opt.data, 'val'), 
                                batch_size=opt.batch_size)
    test_batches = load_records(os.path.join(opt.data, 'test'), 
                                batch_size=opt.batch_size)

    model = RNNModel(num_units=opt.units, 
                     num_layers=opt.layers, 
                     num_cls=n_cls, 
                     dropout=opt.dropout)

    model.model(opt.batch_size).summary()

    model.compile(optimizer=Adam(lr=opt.lr), 
                  loss=CategoricalCrossentropy(), 
                  metrics=[Recall(), CategoricalAccuracy()])

    model.fit(train_batches,
              epochs=opt.epochs,
              callbacks=get_callbacks(opt.p),
              validation_data=val_batches)
    # Testing
    metrics = model.evaluate(test_batches)

    # Saving metrics and setup file
    os.makedirs(os.path.join(opt.p, 'test'), exist_ok=True)
    test_file = os.path.join(opt.p, 'test/test_metrics.json')
    with open(test_file, 'w') as json_file:
        json.dump({'loss': metrics[0], 
                   'recall': metrics[1], 
                   'accuracy':metrics[2]}, json_file, indent=4)

    conf_file = os.path.join(opt.p, 'conf.json')
    with open(conf_file, 'w') as json_file:
        json.dump(vars(opt), json_file, indent=4)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # TRAINING PAREMETERS
    parser.add_argument('--data', default='./data/records/contributions/', type=str,
                        help='Dataset folder containing the records files')
    parser.add_argument('--p', default="./experiments/cont", type=str,
                        help='Proyect path. Here will be stored weights and metrics')
    parser.add_argument('--batch-size', default=512, type=int,
                        help='batch size')
    parser.add_argument('--epochs', default=2000, type=int,
                        help='Number of epochs')
    # ASTROMER HIPERPARAMETERS
    parser.add_argument('--layers', default=2, type=int,
                        help='Number of encoder layers')
    parser.add_argument('--units', default=256, type=int,
                        help='Number of self-attention heads')
    parser.add_argument('--dropout', default=0.25, type=int,
                        help='Dropout applied to the output of the RNN')
    parser.add_argument('--lr', default=1e-3, type=int,
                        help='Optimizer learning rate')

    opt = parser.parse_args()

    train(opt)