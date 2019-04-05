# Copyright 2019 The TensorFlow Authors. All Rights Reserved.
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
"""Runs MNIST using Distribution Strategies."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import logging
import numpy as np
import os

from tensorboard.plugins.core import core_plugin
import tensorboard.program as tb_program

import tensorflow as tf


def get_args():
    """Argument parser.

      Returns:
        Dictionary of arguments.
      """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--working-dir',
        type=str,
        required=True,
        help='GCS location to write checkpoints and export models')
    parser.add_argument(
        '--num-epochs',
        type=float,
        default=5,
        help='number of times to go through the data, default=5')
    parser.add_argument(
        '--batch-size',
        default=128,
        type=int,
        help='number of records to read during each training step, default=128')
    parser.add_argument(
        '--learning-rate',
        default=.01,
        type=float,
        help='learning rate for gradient descent, default=.001')
    parser.add_argument(
        '--verbosity',
        choices=['DEBUG', 'ERROR', 'FATAL', 'INFO', 'WARN'],
        default='INFO')
    args, _ = parser.parse_known_args()
    return args


def get_distribution_strategy():
    cluster_spec = os.environ.get("CLUSTER_SPEC", None)
    if cluster_spec:
        cluster_spec = json.loads(cluster_spec)
        job_index = int(os.environ["TASK_INDEX"])
        job_type = os.environ["JOB_NAME"]
        # Build cluster spec
        os.environ['TF_CONFIG'] = json.dumps(
            {'cluster': cluster_spec,
             'task': {'type': job_type, 'index': job_index}})

        print('Distribution enabled: ', os.environ['TF_CONFIG'])
    else:
        print('Distribution is not enabled')


def create_model(model_dir, config, learning_rate):
    """Creates a Keras Sequential model with layers.

        Args:
          model_dir: (str) file path where training files will be written.
          config: (tf.estimator.RunConfig) Configuration options to save model.
          learning_rate: (int) Learning rate.

        Returns:
          A keras.Model
        """
    l = tf.keras.layers
    model = tf.keras.Sequential(
        [
            l.Reshape(input_shape=(28 * 28,), target_shape=(28, 28, 1)),

            l.Conv2D(filters=6, kernel_size=3, padding='same',
                     use_bias=False),
            # no bias necessary before batch norm
            l.BatchNormalization(scale=False, center=True),
            # no batch norm scaling necessary before "relu"
            l.Activation('relu'),  # activation after batch norm

            l.Conv2D(filters=12, kernel_size=6, padding='same',
                     use_bias=False,
                     strides=2),
            l.BatchNormalization(scale=False, center=True),
            l.Activation('relu'),

            l.Conv2D(filters=24, kernel_size=6, padding='same',
                     use_bias=False,
                     strides=2),
            l.BatchNormalization(scale=False, center=True),
            l.Activation('relu'),

            l.Flatten(),
            l.Dense(200, use_bias=False),
            l.BatchNormalization(scale=False, center=True),
            l.Activation('relu'),
            l.Dropout(0.5),  # Dropout on dense layer only

            l.Dense(10, activation='softmax')
        ])
    # Compile model with learning parameters.
    optimizer = tf.train.GradientDescentOptimizer(learning_rate=learning_rate)
    model.compile(
        optimizer=optimizer,
        loss='sparse_categorical_crossentropy',
        metrics=['accuracy'])
    tf.keras.backend.set_learning_phase(True)
    model.summary()
    estimator = tf.keras.estimator.model_to_estimator(
        keras_model=model, model_dir=model_dir, config=config)
    return estimator


def input_fn(features, labels, batch_size, mode):
    """Input function.

    Args:
      features: (numpy.array) Training or eval data.
      labels: (numpy.array) Labels for training or eval data.
      batch_size: (int)
      mode: tf.estimator.ModeKeys mode

    Returns:
      A tf.estimator.
    """
    # Default settings for training.
    if labels is None:
        inputs = features
    else:
        # Change numpy array shape.
        inputs = (features, labels)
    # Convert the inputs to a Dataset.
    dataset = tf.data.Dataset.from_tensor_slices(inputs)
    if mode == tf.estimator.ModeKeys.TRAIN:
        dataset = dataset.shuffle(1000).repeat().batch(batch_size)
        dataset = dataset.prefetch(100)
    if mode in (tf.estimator.ModeKeys.EVAL, tf.estimator.ModeKeys.PREDICT):
        dataset = dataset.batch(batch_size)
    return dataset


def serving_input_fn():
    """Defines the features to be passed to the model during inference.

    Expects already tokenized and padded representation of sentences

    Returns:
      A tf.estimator.export.ServingInputReceiver
    """
    feature_placeholder = tf.placeholder(tf.float32, [None, 28 * 28])
    features = feature_placeholder
    return tf.estimator.export.TensorServingInputReceiver(features,
                                                          feature_placeholder)


def _get_session_config_from_env_var():
    """Returns a tf.ConfigProto instance with appropriate device_filters set."""

    tf_config = json.loads(os.environ.get('TF_CONFIG', '{}'))

    # GPU limit: TensorFlow by default allocates all GPU memory:
    # If multiple workers run in same host you may see OOM errors:
    # Use as workaround if not using Hadoop 3.1
    # Change percentage accordingly:
    # gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=0.25)

    if (tf_config and 'task' in tf_config and 'type' in tf_config['task'] and
        'index' in tf_config['task']):
        # Master should only communicate with itself and ps.
        if tf_config['task']['type'] == 'master':
            return tf.ConfigProto(device_filters=['/job:ps', '/job:master'])
        # Worker should only communicate with itself and ps.
        elif tf_config['task']['type'] == 'worker':
            return tf.ConfigProto(#gpu_options=gpu_options,
                                  device_filters=[
                                      '/job:ps',
                                      '/job:worker/task:%d' % tf_config['task'][
                                          'index']
                                  ])
    return None


def start_tensorboard(logdir):
    tb = tb_program.TensorBoard(plugins=[core_plugin.CorePluginLoader()])
    port = int(os.getenv('TB_PORT', 6006))
    tb.configure(logdir=logdir, port=port)
    tb.launch()
    logging.info("Starting TensorBoard with --logdir=%s" % logdir)


def train_and_evaluate(args):
    """Helper function: Trains and evaluates model.

    Args:
      args: (dict) Command line parameters passed from task.py
    """
    # Loads data.
    (train_images, train_labels), (
        test_images, test_labels) = tf.keras.datasets.mnist.load_data()

    # Scale values to a range of 0 to 1.
    train_images = train_images / 255.0
    test_images = test_images / 255.0

    # Shape numpy array.
    train_labels = np.asarray(train_labels).astype('int').reshape((-1, 1))
    test_labels = np.asarray(test_labels).astype('int').reshape((-1, 1))

    # Define training steps.
    train_steps = len(train_images) / args.batch_size
    get_distribution_strategy()

    hook = tf.train.ProfilerHook(save_steps=100,
                                 output_dir=args.working_dir,
                                 show_memory=True)

    # Define running config.
    run_config = tf.estimator.RunConfig(
        experimental_distribute=tf.contrib.distribute.DistributeConfig(
            train_distribute=tf.contrib.distribute.ParameterServerStrategy(),
            eval_distribute=tf.contrib.distribute.MirroredStrategy()),
        session_config=_get_session_config_from_env_var(),
        model_dir=args.working_dir,
        save_summary_steps=100,
        log_step_count_steps=100,
        save_checkpoints_steps=500)
    # Create estimator.
    estimator = create_model(
        model_dir=args.working_dir,
        config=run_config,
        learning_rate=args.learning_rate)
    # Create TrainSpec.
    train_spec = tf.estimator.TrainSpec(
        input_fn=lambda: input_fn(
            train_images,
            train_labels,
            args.batch_size,
            mode=tf.estimator.ModeKeys.TRAIN),
        # hooks=[hook], # Uncomment if needed to debug.
        max_steps=train_steps)
    # Create EvalSpec.
    exporter = tf.estimator.FinalExporter('exporter', serving_input_fn)
    eval_spec = tf.estimator.EvalSpec(
        input_fn=lambda: input_fn(
            test_images,
            test_labels,
            args.batch_size,
            mode=tf.estimator.ModeKeys.EVAL),
        steps=None,
        name='mnist-eval',
        exporters=[exporter],
        start_delay_secs=10,
        throttle_secs=10)

    # Launch Tensorboard in a separate thread.
    tf.gfile.MakeDirs(args.working_dir)
    start_tensorboard(args.working_dir)

    # Start training
    tf.estimator.train_and_evaluate(estimator, train_spec, eval_spec)


if __name__ == '__main__':
    args = get_args()
    tf.logging.set_verbosity(args.verbosity)
    train_and_evaluate(args)
