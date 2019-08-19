# TonY [![Build Status](https://travis-ci.org/linkedin/TonY.svg?branch=master)](https://travis-ci.org/linkedin/TonY)

![tony-logo-small](https://user-images.githubusercontent.com/544734/57793050-45b3ff00-76f5-11e9-8cc0-8ebb830b6e78.png)

TonY is a framework to _natively_ run deep learning jobs on [Apache Hadoop](http://hadoop.apache.org/).
It currently supports [TensorFlow](https://github.com/tensorflow/tensorflow), [PyTorch](https://github.com/pytorch/pytorch), and [Horovod](https://github.com/horovod/horovod).
TonY enables running either single node or distributed
training as a Hadoop application. This native connector, together with other TonY features, aims to run
machine learning jobs reliably and flexibly. For a quick overview of TonY and comparisons to other frameworks, please see
[this presentation](https://www.slideshare.net/ssuser72f42a/scaling-deep-learning-on-hadoop-at-linkedin).

## Compatibility Notes

It is recommended to run TonY with [Hadoop 3.1.1](https://hadoop.apache.org/old/releases.html#8+Aug+2018%3A+Release+3.1.1+available) and above. TonY itself is compatible with [Hadoop 2.7.4](https://hadoop.apache.org/docs/r2.7.4/) and above. If you need GPU isolation from TonY, you need [Hadoop 3.1.0](https://hortonworks.com/blog/gpus-support-in-apache-hadoop-3-1-yarn-hdp-3/) or higher.

## Build

### How to build
TonY is built using [Gradle](https://github.com/gradle/gradle). To build TonY, run:

    ./gradlew build

This will automatically run tests, if want to build without running tests, run:

    ./gradlew build -x test

The jar required to run TonY will be located in `./tony-cli/build/libs/`.

## Publishing (for admins)

Follow [this guide](https://blog.sonatype.com/2010/01/how-to-generate-pgp-signatures-with-maven/) to generate a key pair using GPG. Publish your public key.

Create a Nexus account at https://oss.sonatype.org/ and request access to publish to com.linkedin.tony. Here's an example Jira ticket: https://issues.sonatype.org/browse/OSSRH-47350.

Configure your `~/.gradle/gradle.properties` file:

```
# signing plugin uses these
signing.keyId=...
signing.secretKeyRingFile=/home/<ldap>/.gnupg/secring.gpg
signing.password=...

# maven repo credentials
mavenUser=...
mavenPassword=...

# gradle-nexus-staging-plugin uses these
nexusUsername=<sameAsMavenUser>
nexusPassword=<sameAsMavenPassword>
```

Now you can publish and release artifacts by running `./gradlew publish closeAndReleaseRepository`.

## Usage

TonY is a Java library, so it is as simple as running a Java program. There are two ways to launch your deep learning jobs with TonY:
- Use Docker container.
- Use a zipped Python virtual environment.

### Use a Docker container
Note that this requires you have a properly configured Hadoop cluster with Docker support. Check this [documentation](https://hadoop.apache.org/docs/r2.9.1/hadoop-yarn/hadoop-yarn-site/DockerContainers.html) if you are unsure how to set it up. Assuming you have properly set up your Hadoop cluster with Docker container runtime, you should have already built a proper Docker image with required Hadoop configurations. The next thing you need is to install your Python dependencies inside your Docker image - TensorFlow or PyTorch.

Below is a folder structure of what you need to launch the job:

    MyJob/
      > src/
        > models/
          mnist_distributed.py
      tony.xml
      tony-cli-0.1.5-all.jar

The `src/` folder would contain all your training script. The `tony.xml` is used to config your training job. Specifically for using Docker as the container runtime, your configuration should be similar to something below:

    $ cat MyJob/tony.xml
    <configuration>
      <property>
        <name>tony.worker.instances</name>
        <value>4</value>
      </property>
      <property>
        <name>tony.worker.memory</name>
        <value>4g</value>
      </property>
      <property>
        <name>tony.worker.gpus</name>
        <value>1</value>
      </property>
      <property>
        <name>tony.ps.memory</name>
        <value>3g</value>
      </property>
      <property>
        <name>tony.application.docker.enabled</name>
        <value>true</value>
      </property>
      <property>
        <name>tony.application.docker.image</name>
        <value>YOUR_DOCKER_IMAGE_NAME</value>
      </property>
    </configuration>

For a full list of configurations, please see the [wiki](https://github.com/linkedin/TonY/wiki/TonY-Configurations).

Now you're ready to launch your job:

    $ java -cp "`hadoop classpath --glob`:MyJob/*:MyJob/" \
            com.linkedin.tony.cli.ClusterSubmitter \
            -executes models/mnist_distributed.py \
            -task_params '--input_dir /path/to/hdfs/input --output_dir /path/to/hdfs/output' \
            -src_dir src \
            -python_binary_path /home/user_name/python_virtual_env/bin/python

### Use a zipped Python virtual environment

The difference between this approach and the one with Docker is
- You don't need to set up your Hadoop cluster with Docker support.
- There is no requirement on a Docker image registry.

As you know, nothing comes for free. If you don't want to bother setting your cluster with Docker support, you'd need to prepare a zipped virtual environment for your job and your cluster should have the same OS version as the computer which builds the Python virtual environment.

#### Python virtual environment in a zip

    $ unzip -Z1 my-venv.zip | head -n 10
      Python/
      Python/bin/
      Python/bin/rst2xml.py
      Python/bin/wheel
      Python/bin/rst2html5.py
      Python/bin/rst2odt.py
      Python/bin/rst2s5.py
      Python/bin/pip2.7
      Python/bin/saved_model_cli
      Python/bin/rst2pseudoxml.pyc

#### TonY jar and tony.xml

    MyJob/
      > src/
        > models/
          mnist_distributed.py
      tony.xml
      tony-cli-0.1.5-all.jar
      my-venv.zip # The additional file you need.

A similar `tony.xml` but without Docker related configurations:

    $ cat tony/tony.xml
    <configuration>
      <property>
        <name>tony.worker.instances</name>
        <value>4</value>
      </property>
      <property>
        <name>tony.worker.memory</name>
        <value>4g</value>
      </property>
      <property>
        <name>tony.worker.gpus</name>
        <value>1</value>
      </property>
      <property>
        <name>tony.ps.memory</name>
        <value>3g</value>
      </property>
    </configuration>

Then you can launch your job:

    $ java -cp "`hadoop classpath --glob`:MyJob/*:MyJob" \
                com.linkedin.tony.cli.ClusterSubmitter \
                -executes models/mnist_distributed.py \ # relative path to model program inside the src_dir
                -task_params '--input_dir /path/to/hdfs/input --output_dir /path/to/hdfs/output \
                -python_venv my-venv.zip \
                -python_binary_path Python/bin/python \  # relative path to the Python binary inside the my-venv.zip
                -src_dir src

## TonY arguments
The command line arguments are as follows:

| Name               | Required? | Example                                           | Meaning                                                                                                                                                                                                           |
|--------------------|-----------|---------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| executes           | yes       | --executes model/mnist.py                         | Location to the entry point of your training code.                                                                                                                                                                |
| src_dir            | yes       | --src src/                                        | Specifies the name of the root directory locally which contains all of your python model source code. This directory will be copied to all worker node.                                                           |
| task_params        | no        | --input_dir /hdfs/input --output_dir /hdfs/output | The command line arguments which will be passed to your entry point                                                                                                                                               |
| python_venv        | no        | --python_venv venv.zip                            | Path to the *zipped* local Python virtual environment                                                                                                                                                             |
| python_binary_path | no        | --python_binary_path Python/bin/python            | Used together with python_venv, describes the relative path in your python virtual environment which contains the python binary, or an absolute path to use a python binary already installed on all worker nodes |
| shell_env          | no        | --shel_env LD_LIBRARY_PATH=/usr/local/lib64/      | Specifies key-value pairs for environment variables which will be set in your python worker/ps processes.                                                                                                         |
| conf_file          | no        | --conf_file tony-local.xml                        | Location of a TonY configuration file.                                                                                                                                                                            |
| conf               | no        | --conf tony.application.security.enabled=false    | Override configurations from your configuration file via command line

## TonY configurations

There are multiple ways to specify configurations for your TonY job. As above, you can create an XML file called `tony.xml`
and add its parent directory to your java classpath.

Alternatively, you can pass `-conf_file <name_of_conf_file>` to the java command line if you have a file not named `tony.xml`
containing your configurations. (As before, the parent directory of this file must be added to the java classpath.)

If you wish to override configurations from your configuration file via command line, you can do so by passing `-conf <tony.conf.key>=<tony.conf.value>` argument pairs on the command line.

Please check our [wiki](https://github.com/linkedin/TonY/wiki/TonY-Configurations) for all TonY configurations and their default values.

## TonY Examples

Below are examples to run distributed deep learning jobs with TonY:
- [Distributed MNIST with TensorFlow](https://github.com/linkedin/TonY/tree/master/tony-examples/mnist-tensorflow)
- [Distributed MNIST with PyTorch](https://github.com/linkedin/TonY/tree/master/tony-examples/mnist-pytorch)
- [TonY in Google Cloud Platform](https://github.com/linkedin/TonY/tree/master/tony-examples/tony-in-gcp)
- [TonY in Azkaban video](https://youtu.be/DM89y8BGFaY)

## More information

For more information about TonY, check out the following:
- [TonY presentation at DataWorks Summit '19 in Washington, D.C.](https://www.slideshare.net/ssuser72f42a/scaling-deep-learning-on-hadoop-at-linkedin)
- [TonY OpML '19 paper](https://arxiv.org/abs/1904.01631)
- [TonY LinkedIn Engineering blog post](https://engineering.linkedin.com/blog/2018/09/open-sourcing-tony--native-support-of-tensorflow-on-hadoop)


## FAQ

1. My tensorflow process hangs with  
    ```
    2018-09-13 03:02:31.538790: E tensorflow/core/distributed_runtime/master.cc:272] CreateSession failed because worker /job:worker/replica:0/task:0 returned error: Unavailable: OS Error
    INFO:tensorflow:An error was raised while a session was being created. This may be due to a preemption of a connected worker or parameter server. A new session will be created. Error: OS Error
    INFO:tensorflow:Graph was finalized.
    2018-09-13 03:03:33.792490: I tensorflow/core/distributed_runtime/master_session.cc:1150] Start master session ea811198d338cc1d with config: 
    INFO:tensorflow:Waiting for model to be ready.  Ready_for_local_init_op:  Variables not initialized: conv1/Variable, conv1/Variable_1, conv2/Variable, conv2/Variable_1, fc1/Variable, fc1/Variable_1, fc2/Variable, fc2/Variable_1, global_step, adam_optimizer/beta1_power, adam_optimizer/beta2_power, conv1/Variable/Adam, conv1/Variable/Adam_1, conv1/Variable_1/Adam, conv1/Variable_1/Adam_1, conv2/Variable/Adam, conv2/Variable/Adam_1, conv2/Variable_1/Adam, conv2/Variable_1/Adam_1, fc1/Variable/Adam, fc1/Variable/Adam_1, fc1/Variable_1/Adam, fc1/Variable_1/Adam_1, fc2/Variable/Adam, fc2/Variable/Adam_1, fc2/Variable_1/Adam, fc2/Variable_1/Adam_1, ready: None
    ```  
    Why?
    
    Try adding the path to your libjvm.so shared library to your LD_LIBRARY_PATH environment variable for your workers. See above for an example.

2. How do I configure arbitrary TensorFlow job types?

    Please see the [wiki](https://github.com/linkedin/TonY/wiki/TonY-Configurations#task-configuration) on TensorFlow task configuration for details.
