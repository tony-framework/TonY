### Running Examples

To run the examples here, you need to:

* Build a Python virtual environment with TensorFlow 2.3 installed
* Install Hadoop 2.10+ for Hadoop v2 or 3.1.1+ for Hadoop v3

If you don't have security enabled, you'll also need to provide a custom config file with security turned off.


### Building a Python virtual environment with TensorFlow

TonY requires a Python virtual environment zip with TensorFlow and any needed Python libraries already installed.

```
wget https://files.pythonhosted.org/packages/33/bc/fa0b5347139cd9564f0d44ebd2b147ac97c36b2403943dbee8a25fd74012/virtualenv-16.0.0.tar.gz
tar xf virtualenv-16.0.0.tar.gz

# Make sure to install using Python 3, as TensorFlow only provides Python 3 artifacts
python virtualenv-16.0.0/virtualenv.py venv
. venv/bin/activate
pip install tensorflow
zip -r venv.zip venv
```

### TensorFlow version: 

 - Version 2.3

**Note:** If you require a past version of TensorFlow and TensorBoard, take a look at [this](https://github.com/linkedin/TonY/issues/42) issue.


### Installing Hadoop

TonY only requires YARN, not HDFS. Please see the [open-source documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html) on how to set YARN up.


### Configuration

Below is an example config file to request 3 workers. We also assume our Hadoop cluster
does NOT have security enabled (e.g.: for local testing), so we disable TonY's security support.

```
<configuration>
  <property>
    <name>tony.worker.instances</name>
    <value>3</value>
  </property>
  <property>
    <name>tony.worker.memory</name>
    <value>4g</value>
  </property>
  <property>
    <name>tony.application.security.enabled</name>
    <value>false</value>
  </property>
</configuration>
```

For the instructions below, we assume this file is named `tony-test.xml`.


### Running MNIST Tensorflow example:

Once you've installed Hadoop and built your Python virtual environment zip, you can run an example as follows:

```
gradlew :tony-cli:build

java -cp `hadoop classpath`:/path/to/TonY/tony-cli/build/libs/tony-cli-x.x.x-all.jar com.linkedin.tony.cli.ClusterSubmitter \
--python_venv=/path/to/venv.zip \
--src_dir=/path/to/TonY/tony-examples/mnist-tensorflow \
--executes=mnist_distributed.py \ # relative path inside src/
--task_params="--steps 1000 --data_dir /tmp/data --working_dir /tmp/model" \ # You can use your HDFS path here.
--conf_file=/path/to/tony-test.xml \
--python_binary_path=venv/bin/python # relative path inside venv.zip
```

### Running MNIST Keras example:

You could also alternative try this MNIST Keras example:

```
gradlew :tony-cli:build

java -cp `hadoop classpath`:/path/to/TonY/tony-cli/build/libs/tony-cli-x.x.x-all.jar com.linkedin.tony.cli.ClusterSubmitter \
--python_venv=/path/to/venv.zip \
--src_dir=/path/to/TonY/tony-examples/mnist-tensorflow \
--executes=mnist_keras_distributed.py \ # relative path inside src/
--task_params="--working-dir /tmp/model" \ # You can use your HDFS path here.
--conf_file=/path/to/tony-test.xml \
--python_binary_path=venv/bin/python # relative path inside venv.zip
```

We have tested this example with 3 Workers (4GB RAM + 1 vCPU) using MultiWorkerMirror strategy

### Tensorboard Usage
TonY supports two modes(custom and sidecar) to start tensorboard.
1. [Custom] Allow users to start tensorboard in code, more details can be found in mnist_distributed.py example.
2. [Sidecar] Using the built-in tensorboard, it will start extra executor to running tensorboard by TonY. Only one thing to do is specify the log dir in tony xml, like as follows

```
<configuration>
  ....
  <property>
    <name>tony.application.tensorboard-log-dir</name>
    <value>/tmp/xxxxxxx</value>
  </property>
</configuration>
```