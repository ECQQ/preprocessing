import tensorflow as tf 

class MaskedACC(tf.keras.metrics.Metric):
    def __init__(self, name="Accuracy", **kwargs):
        super(MaskedACC, self).__init__(name=name, **kwargs)
        self.acc_value = tf.constant(0.)

    def update_state(self, y_true, y_pred, sample_weight=None):     
        real = tf.slice(y_true, [0,0,0], [-1,-1,1])
        mask = tf.slice(y_true, [0,0,1], [-1,-1,1])
        real = tf.cast(real, dtype=tf.int64)
        mask = tf.cast(mask, dtype=tf.bool)
        
        real = tf.squeeze(real)
        mask = tf.squeeze(mask)
        accuracies = tf.equal(real, tf.argmax(y_pred, axis=2))
        accuracies = tf.math.logical_and(mask, accuracies)
        accuracies = tf.cast(accuracies, dtype=tf.float32)
        mask = tf.cast(mask, dtype=tf.float32)
        self.acc_value = tf.reduce_sum(accuracies)/tf.reduce_sum(mask)

    def result(self):
        return self.acc_value


  