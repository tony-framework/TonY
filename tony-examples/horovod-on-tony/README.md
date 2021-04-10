### Running Examples
This example shows how to run a simple Horovod program on TonY.
Requirements:
1. Build a Docker runtime container(required Hadoop configurations) with TF2.x installed and Horovod 0.21.3+
2. Install Hadoop 3.1.1+

If you don't have security enabled, you'll also need to provide a custom config file with security turned off.

### Build a Docker runtime container
1. Prepare Dockerfile
```
FROM ${YOUR_BASIC_HADOOP_CONTAINER_IMAGE}

RUN pip3 install tensorflow==2.4.1 \ 
    && HOROVOD_WITH_GLOO=1 HOROVOD_WITH_TENSORFLOW=1 pip3 install horovod[tensorflow]
```
2. Build image
```
docker build -t docker.io/bigdata/horovod-test-1:v1 .
```
3. Push to docker registry
```
docker push docker.io/bigdata/horovod-test-1:v1
```

For the instructions below, we assume this docker image has been pushed to docker registry which can be access by Hadoop nodemanager, and this image is named __docker.io/bigdata/horovod-test-1:v1__

### Install Hadoop 3.1.1+
TonY only requires YARN, not HDFS. Please see the open-source documentation on how to set YARN up.

### Config TonY job for Horovod
If your Hadoop cluster is not running with security enabled (e.g.: for local testing), you need to disable the security check. Here is a sample of the config:
```
<configuration>
  <property>
    <name>tony.worker.instances</name>
    <value>4</value>
  </property>
  <property>
    <name>tony.worker.memory</name>
    <value>3g</value>
  </property>
  <property>
    <name>tony.docker.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>tony.docker.containers.image</name>
    <value>docker.io/bigdata/horovod-test-1:v1</value>
  </property>
</configuration>
```

For the instructions below, we assume this file is named __tony-test.xml__

### Running an example
```
gradlew :tony-cli:build

java -cp `hadoop classpath`:/path/to/TonY/tony-cli/build/libs/tony-cli-x.x.x-all.jar com.linkedin.tony.cli.ClusterSubmitter \
--src_dir=/path/to/TonY/tony-examples/horovod-on-tony \
--executes=tensorflow2_mnist.py \
--conf_file=/path/to/tony-test.xml \
--python_binary_path=python3
```

