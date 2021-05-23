# Copyright 2021 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""A deep MNIST classifier using DNNClassifier in tf.Estimator """

import numpy as np
import tensorflow as tf
from absl import flags, app, logging


def build_mnist():
    (x_train, y_train), (x_test, y_test) = tf.keras.datasets.mnist.load_data()
    x_train = x_train.astype(np.int)
    y_train = y_train.astype(np.int)
    x_test = x_test.astype(np.int)
    y_test = y_test.astype(np.int)
    return x_train, y_train, x_test, y_test


def build_config():
    return tf.estimator.RunConfig(
        keep_checkpoint_max=3,
        save_checkpoints_steps=1000,
        save_summary_steps=500,
        log_step_count_steps=500
    )


def build_feature_columns():
    return [tf.feature_column.numeric_column("feature", shape=[28, 28])]


def build_classifier(feature_columns, run_config):
    return tf.estimator.DNNClassifier(
        feature_columns=feature_columns,
        hidden_units=[256, 128],
        optimizer=tf.keras.optimizers.Adagrad(learning_rate=FLAGS.learning_rate, epsilon=1e-5),
        n_classes=10,
        dropout=FLAGS.dropout,
        model_dir=FLAGS.model_dir,
        config=run_config
    )


def build_input_fn(x_train, y_train, x_test, y_test):
    train_input_fn = tf.compat.v1.estimator.inputs.numpy_input_fn(
        {"feature": x_train},
        y_train,
        batch_size=FLAGS.batch_size,
        shuffle=True, num_epochs=FLAGS.train_epoch)

    eval_input_fn = tf.compat.v1.estimator.inputs.numpy_input_fn(
        {"feature": x_test},
        y_test,
        batch_size=FLAGS.batch_size,
        shuffle=False,
        num_epochs=1)

    return train_input_fn, eval_input_fn


def build_spec(train_input_fn, eval_input_fn):
    train_spec = tf.estimator.TrainSpec(
        input_fn=train_input_fn,
        max_steps=FLAGS.max_steps,
        hooks=[])

    eval_spec = tf.estimator.EvalSpec(
        input_fn=eval_input_fn,
        steps=500,
        throttle_secs=60)

    return train_spec, eval_spec


def main(argv):
    del argv
    logging.set_verbosity("INFO")

    run_config = build_config()
    x_train, y_train, x_test, y_test = build_mnist()
    feature_columns = build_feature_columns()
    classifier = build_classifier(feature_columns, run_config)
    train_input_fn, eval_input_fn = build_input_fn(x_train, y_train, x_test, y_test)
    train_spec, eval_spec = build_spec(train_input_fn, eval_input_fn)

    tf.estimator.train_and_evaluate(classifier, train_spec, eval_spec)
    if run_config.task_type == "chief":
        classifier.evaluate(eval_input_fn)


if __name__ == '__main__':
    flags.DEFINE_string('model_dir', 'output', 'path of model output')
    flags.DEFINE_float('dropout', 0.5, 'dropout of this model')
    flags.DEFINE_float('learning_rate', 0.001, 'learning rate of this model')
    flags.DEFINE_integer('train_epoch', 500, 'train epoch of this model')
    flags.DEFINE_integer('batch_size', 64, 'batch size of this model')
    flags.DEFINE_integer('max_steps', 300000, 'batch size of this model')
    FLAGS = flags.FLAGS

    app.run(main)
