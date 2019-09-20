### Running Examples

This examples haven't yet tested on GPU.
To run the examples here, you need to:

* install Hadoop 2.1.16

If you don't have security enabled, you'll also need to provide a custom config file with security turned off.


### Building a Python environment with MXNet(CPU Version)
You must make zipped all Python files. We assumed this zipped file is called MxNetPython3.5.zip.
``` bashshell
./build_python3.5.sh
./make_zip.sh
```


### Installing Hadoop

TonY only requires YARN, not HDFS. Please see the [open-source documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html) on how to set YARN up.


### Config TonY job for MXNet(CPU Version)

In MXNet framework, an cluster is consistied of scheduler, server and worker. Commonly scheduler is cheif node, server is parameter server.  You need server(parameter) servers for distribute MXNet training and if your Hadoop cluster is not running with security enabled (e.g.: for local testing), you need to disable the security check. Please note that tony.ps.instance must set 0. Here is a sample of the config:

```
<configuration>
  <property>
    <name>tony.application.security.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>tony.scheduler.instances</name>
    <value>1</value>
  </property>
  <property>
    <name>tony.server.instances</name>
    <value>2</value>
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
    <name>tony.worker.vcores</name>
    <value>2</value>
  </property>
  <property>
    <name>tony.worker.memory</name>
    <value>4G</value>
  </property>
  <property>
    <name>tony.application.framework</name>
    <value>mxnet</value>
  </property>
  <property>
    <name>tony.application.name</name>
    <value>mxnet - linearregression</value>
  </property>
</configuration>
```

For the instructions below, we assume this file is named `tony-mxnet-job.xml`.


### Running an example

Once you've installed Hadoop and built your Python environment zip, you can run an example as follows:

``` bashshell
gradlew :tony-cli:build
./tony_mxnet_submit.sh   # this script will call python tony_mxnet_submit.py
```

