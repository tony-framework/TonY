# Copyright 2015 The TensorFlow Authors. All Rights Reserved.
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

"""A deep MNIST classifier using convolutional layers.

This example was adapted from
https://github.com/tensorflow/tensorflow/blob/master/tensorflow/examples/tutorials/mnist/mnist_deep.py.

Each worker reads the full MNIST dataset and asynchronously trains a CNN with dropout and using the Adam optimizer,
updating the model parameters on shared parameter servers.

The current training accuracy is printed out after every 100 steps.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tensorflow.examples.tutorials.mnist import input_data

import json
import logging
import os
import sys

import tensorboard.program as tb_program
import tensorflow as tf

# Environment variable containing port to launch TensorBoard on, set by TonY.
TB_PORT_ENV_VAR = 'TB_PORT'

# Input/output directories
tf.flags.DEFINE_string('data_dir', '/tmp/tensorflow/mnist/input_data',
                       'Directory for storing input data')
tf.flags.DEFINE_string('working_dir', '/tmp/tensorflow/mnist/working_dir',
                       'Directory under which events and output will be '
                       'stored (in separate subdirectories).')

# Training parameters
tf.flags.DEFINE_integer("steps", 1500,
                        "The number of training steps to execute.")
tf.flags.DEFINE_integer("batch_size", 64, "The batch size per step.")

FLAGS = tf.flags.FLAGS


def deepnn(x):
    """deepnn builds the graph for a deep net for classifying digits.

    Args:
      x: an input tensor with the dimensions (N_examples, 784), where 784 is the
      number of pixels in a standard MNIST image.

    Returns:
      A tuple (y, keep_prob). y is a tensor of shape (N_examples, 10), with values
      equal to the logits of classifying the digit into one of 10 classes (the
      digits 0-9). keep_prob is a scalar placeholder for the probability of
      dropout.
    """
    # Reshape to use within a convolutional neural net.
    # Last dimension is for "features" - there is only one here, since images are
    # grayscale -- it would be 3 for an RGB image, 4 for RGBA, etc.
    with tf.name_scope('reshape'):
        x_image = tf.reshape(x, [-1, 28, 28, 1])

    # First convolutional layer - maps one grayscale image to 32 feature maps.
    with tf.name_scope('conv1'):
        W_conv1 = weight_variable([5, 5, 1, 32])
        b_conv1 = bias_variable([32])
        h_conv1 = tf.nn.relu(conv2d(x_image, W_conv1) + b_conv1)

    # Pooling layer - downsamples by 2X.
    with tf.name_scope('pool1'):
        h_pool1 = max_pool_2x2(h_conv1)

    # Second convolutional layer -- maps 32 feature maps to 64.
    with tf.name_scope('conv2'):
        W_conv2 = weight_variable([5, 5, 32, 64])
        b_conv2 = bias_variable([64])
        h_conv2 = tf.nn.relu(conv2d(h_pool1, W_conv2) + b_conv2)

    # Second pooling layer.
    with tf.name_scope('pool2'):
        h_pool2 = max_pool_2x2(h_conv2)

    # Fully connected layer 1 -- after 2 round of downsampling, our 28x28 image
    # is down to 7x7x64 feature maps -- maps this to 1024 features.
    with tf.name_scope('fc1'):
        W_fc1 = weight_variable([7 * 7 * 64, 1024])
        b_fc1 = bias_variable([1024])

        h_pool2_flat = tf.reshape(h_pool2, [-1, 7 * 7 * 64])
        h_fc1 = tf.nn.relu(tf.matmul(h_pool2_flat, W_fc1) + b_fc1)

    # Dropout - controls the complexity of the model, prevents co-adaptation of
    # features.
    with tf.name_scope('dropout'):
        keep_prob = tf.placeholder(tf.float32)
        h_fc1_drop = tf.nn.dropout(h_fc1, keep_prob)

    # Map the 1024 features to 10 classes, one for each digit
    with tf.name_scope('fc2'):
        W_fc2 = weight_variable([1024, 10])
        b_fc2 = bias_variable([10])

        y_conv = tf.matmul(h_fc1_drop, W_fc2) + b_fc2
    return y_conv, keep_prob


def conv2d(x, W):
    """conv2d returns a 2d convolution layer with full stride."""
    return tf.nn.conv2d(x, W, strides=[1, 1, 1, 1], padding='SAME')


def max_pool_2x2(x):
    """max_pool_2x2 downsamples a feature map by 2X."""
    return tf.nn.max_pool(x, ksize=[1, 2, 2, 1],
                          strides=[1, 2, 2, 1], padding='SAME')


def weight_variable(shape):
    """weight_variable generates a weight variable of a given shape."""
    initial = tf.truncated_normal(shape, stddev=0.1)
    return tf.Variable(initial)


def bias_variable(shape):
    """bias_variable generates a bias variable of a given shape."""
    initial = tf.constant(0.1, shape=shape)
    return tf.Variable(initial)


def create_model():
    """Creates our model and returns the target nodes to be run or populated"""
    # Create the model
    x = tf.placeholder(tf.float32, [None, 784])

    # Define loss and optimizer
    y_ = tf.placeholder(tf.int64, [None])

    # Build the graph for the deep net
    y_conv, keep_prob = deepnn(x)

    with tf.name_scope('loss'):
        cross_entropy = tf.losses.sparse_softmax_cross_entropy(labels=y_,
                                                               logits=y_conv)
        cross_entropy = tf.reduce_mean(cross_entropy)

    global_step = tf.train.get_or_create_global_step()
    with tf.name_scope('adam_optimizer'):
        train_step = tf.train.AdamOptimizer(1e-4).minimize(cross_entropy,
                                                           global_step=global_step)

    with tf.name_scope('accuracy'):
        correct_prediction = tf.equal(tf.argmax(y_conv, 1), y_)
        correct_prediction = tf.cast(correct_prediction, tf.float32)
    accuracy = tf.reduce_mean(correct_prediction)

    tf.summary.scalar('cross_entropy_loss', cross_entropy)
    tf.summary.scalar('accuracy', accuracy)

    merged = tf.summary.merge_all()

    return x, y_, keep_prob, global_step, train_step, accuracy, merged


def start_tensorboard(logdir):
    tb = tb_program.TensorBoard()
    port = int(os.getenv(TB_PORT_ENV_VAR, 6006))
    tb.configure(logdir=logdir, port=port)
    tb.launch()
    logging.info("Starting TensorBoard with --logdir=%s" % logdir)


def main(_):
    logging.getLogger().setLevel(logging.INFO)

    cluster_spec_str = os.environ["CLUSTER_SPEC"]
    cluster_spec = json.loads(cluster_spec_str)
    ps_hosts = cluster_spec['ps']
    worker_hosts = cluster_spec['worker']

    # Create a cluster from the parameter server and worker hosts.
    cluster = tf.train.ClusterSpec({"ps": ps_hosts, "worker": worker_hosts})

    # Create and start a server for the local task.
    job_name = os.environ["JOB_NAME"]
    task_index = int(os.environ["TASK_INDEX"])
    server = tf.train.Server(cluster, job_name=job_name, task_index=task_index)

    if job_name == "ps":
        server.join()
    elif job_name == "worker":
        # Create our model graph. Assigns ops to the local worker by default.
        with tf.device(tf.train.replica_device_setter(
                worker_device="/job:worker/task:%d" % task_index,
                cluster=cluster)):
            features, labels, keep_prob, global_step, train_step, accuracy, \
            merged = create_model()

        if task_index is 0:  # chief worker
            tf.gfile.MakeDirs(FLAGS.working_dir)
            start_tensorboard(FLAGS.working_dir)

        # The StopAtStepHook handles stopping after running given steps.
        hooks = [tf.train.StopAtStepHook(num_steps=FLAGS.steps)]

        # Filter all connections except that between ps and this worker to
        # avoid hanging issues when one worker finishes. We are using
        # asynchronous training so there is no need for the workers to
        # communicate.
        config_proto = tf.ConfigProto(
            device_filters=['/job:ps', '/job:worker/task:%d' % task_index])

        with tf.train.MonitoredTrainingSession(master=server.target,
                                               is_chief=(task_index == 0),
                                               checkpoint_dir=FLAGS.working_dir,
                                               hooks=hooks,
                                               config=config_proto) as sess:
            # Import data
            logging.info('Extracting and loading input data...')
            mnist = input_data.read_data_sets(FLAGS.data_dir)

            # Train
            logging.info('Starting training')
            i = 0
            while not sess.should_stop():
                batch = mnist.train.next_batch(FLAGS.batch_size)
                if i % 100 == 0:
                    step, _, train_accuracy = sess.run(
                        [global_step, train_step, accuracy],
                        feed_dict={features: batch[0], labels: batch[1],
                                   keep_prob: 1.0})
                    logging.info('Step %d, training accuracy: %g' % (
                    step, train_accuracy))
                else:
                    sess.run([global_step, train_step],
                             feed_dict={features: batch[0], labels: batch[1],
                                        keep_prob: 0.5})
                i += 1

        logging.info('Done training!')
        sys.exit()


if __name__ == '__main__':
    tf.app.run()
