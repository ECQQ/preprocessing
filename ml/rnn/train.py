import tensorflow as tf 
import argparse
import json
import os 

from core.data import load_records
from core.model import RNNModel
from core.ae import RAE
from core.callbacks import get_callbacks
from core.losses import MaskedCrossEntropy
from core.metrics import MaskedACC

from tensorflow.keras.losses import CategoricalCrossentropy
from tensorflow.keras.metrics import Recall, CategoricalAccuracy
from tensorflow.keras.optimizers import RMSprop, Adam

from transformers import XLMTokenizer
tokenizer = XLMTokenizer.from_pretrained("xlm-mlm-100-1280")

def train(opt):
    train_batches, n_cls = load_records(os.path.join(opt.data, 'train'), 
                                        batch_size=opt.batch_size, 
                                        return_cls=True)
    val_batches  = load_records(os.path.join(opt.data, 'val'), 
                                batch_size=opt.batch_size)
    test_batches = load_records(os.path.join(opt.data, 'test'), 
                                batch_size=opt.batch_size)

    if opt.model == 'ae':
        model = RAE(num_units=opt.units, 
                    num_layers=opt.layers, 
                    voc_size=tokenizer.vocab_size, 
                    zdim=opt.zdim,
                    dropout=opt.dropout)
        loss = MaskedCrossEntropy()
        metrics = [MaskedACC()]

    if opt.model == 'rnn':
        model = RNNModel(num_units=opt.units, 
                         num_layers=opt.layers, 
                         num_cls=n_cls, 
                         dropout=opt.dropout)
        loss = CategoricalCrossentropy()
        metrics = [Recall(), CategoricalAccuracy()]


    model.model(opt.batch_size).summary()

    model.compile(optimizer=Adam(lr=opt.lr), 
                  loss=loss, 
                  metrics=metrics)

    model.fit(train_batches,
              epochs=opt.epochs,
              callbacks=get_callbacks(opt.p),
              validation_data=val_batches)
    # # Testing
    # metrics = model.evaluate(test_batches)

    # # Saving metrics and setup file
    # os.makedirs(os.path.join(opt.p, 'test'), exist_ok=True)
    # test_file = os.path.join(opt.p, 'test/test_metrics.json')
    # with open(test_file, 'w') as json_file:
    #     json.dump({'loss': metrics[0], 
    #                'recall': metrics[1], 
    #                'accuracy':metrics[2]}, json_file, indent=4)

    # conf_file = os.path.join(opt.p, 'conf.json')
    # with open(conf_file, 'w') as json_file:
    #     json.dump(vars(opt), json_file, indent=4)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # TRAINING PAREMETERS
    parser.add_argument('--data', default='./data/records/contrib_bert_2/', type=str,
                        help='Dataset folder containing the records files')
    parser.add_argument('--p', default="./experiments/test", type=str,
                        help='Proyect path. Here will be stored weights and metrics')
    parser.add_argument('--batch-size', default=64, type=int,
                        help='batch size')
    parser.add_argument('--epochs', default=2000, type=int,
                        help='Number of epochs')
    # MODEL HIPERPARAMETERS
    parser.add_argument('--layers', default=2, type=int,
                        help='Number of encoder layers')
    parser.add_argument('--units', default=128, type=int,
                        help='Number of units within the recurrent unit(s)')
    parser.add_argument('--zdim', default=15, type=int,
                        help='Latent space dimensionality')
    parser.add_argument('--dropout', default=0.25, type=int,
                        help='Dropout applied to the output of the RNN')
    parser.add_argument('--lr', default=1e-3, type=int,
                        help='Optimizer learning rate')
    parser.add_argument('--model', default='ae', type=str,
                        help='autoencoder (ae) or recurrent neural network (rnn)')

    opt = parser.parse_args()

    train(opt)