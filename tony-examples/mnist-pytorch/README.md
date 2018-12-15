### Running Examples

To run the examples here, you need to:

* build a Python virtual environment with PyTorch 0.4.* installed
* install Hadoop 3.1.1+

If you don't have security enabled, you'll also need to provide a custom config file with security turned off.


### Building a Python virtual environment with PyTorch

TonY requires a Python virtual environment zip with PyTorch and any needed Python libraries already installed.

```
wget https://files.pythonhosted.org/packages/33/bc/fa0b5347139cd9564f0d44ebd2b147ac97c36b2403943dbee8a25fd74012/virtualenv-16.0.0.tar.gz
tar xf virtualenv-16.0.0.tar.gz

python virtualenv-16.0.0/virtualenv.py venv
. venv/bin/activate
pip install pytorch==0.4.0
zip -r venv.zip venv
```


### Installing Hadoop

TonY only requires YARN, not HDFS. Please see the [open-source documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html) on how to set YARN up.


### Config TonY job for PyTorch

You don't need parameter servers for distributed PyTorch training and if your Hadoop cluster is not running with security enabled (e.g.: for local testing), you
need to disable the security check. Here is a sample of the config:

```
<configuration>
  <property>
    <name>tony.application.security.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>tony.ps.instances</name>
    <value>0</value>
  </property>
  <property>
    <name>tony.worker.instances</name>
    <value>2</value>
  </property>
  <property>
    <name>tony.application.framework</name>
    <value>pytorch</value>
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
--src_dir=/path/to/TonY/tony-examples/mnist-pytorch \
--executes=mnist_distributed.py \
--conf_file=/path/to/tony-test.xml \
--python_binary_path=bin/python  # relative path inside venv.zip
```