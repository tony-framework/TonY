### Running Examples

To run the examples here, you need to:

* build a Python virtual environment with TensorFlow 1.9.0 installed
* install Hadoop 3.1.1+

If you don't have security enabled, you'll also need to provide a custom config file with security turned off.


### Building a Python virtual environment with TensorFlow

TonY requires a Python virtual environment zip with TensorFlow and any needed Python libraries already installed.

```
wget https://files.pythonhosted.org/packages/33/bc/fa0b5347139cd9564f0d44ebd2b147ac97c36b2403943dbee8a25fd74012/virtualenv-16.0.0.tar.gz
tar xf virtualenv-16.0.0.tar.gz

# Make sure to install using Python 3, as TensorFlow only provides Python 3 artifacts
python virtualenv-16.0.0/virtualenv.py venv
. venv/bin/activate
pip install tensorflow==1.9.0
zip -r venv.zip venv
```


### Installing Hadoop

TonY only requires YARN, not HDFS. Please see the [open-source documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html) on how to set YARN up.


### Disabling security

If your Hadoop cluster is not running with security enabled (e.g.: for local testing), you can disable security by creating a config file as follows:

```
<configuration>
  <property>
    <name>tony.application.security.enabled</name>
    <value>false</value>
  </property>
</configuration>
```

For the instructions below, we assume this file is named `tony-test.xml`.


### Running an example

Once you've installed Hadoop and built your Python virtual environment zip, you can run an example as follows:

```
gradlew :tony-cli:build

java -cp `hadoop classpath`:/path/to/TonY/tony-cli/build/libs/tony-cli-x.x.x-all.jar com.linkedin.tony.cli.ClusterSubmitter \
--python_venv=/path/to/venv.zip \
--src_dir=/path/to/TonY/tony-examples/mnist \
--executes=/path/to/TonY/tony-examples/mnist/mnist_distributed.py \
--conf_file=/path/to/tony-test.xml \
--python_binary_path=venv/bin/python
```