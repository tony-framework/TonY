# TonY

TensorFlow on YARN (TonY) is a framework to _natively_ run [TensorFlow](https://github.com/tensorflow/tensorflow) 
on [Apache Hadoop](http://hadoop.apache.org/). TonY enables running either single node or distributed TensorFlow 
training as a Hadoop application. This native connector, together with other TonY features, aims to run
TensorFlow jobs reliably and flexibly.

## Build
TonY is built using [Gradle](https://github.com/gradle/gradle). To build TonY, run:

    ./gradlew build

The jar required to run TonY will be located in `./tony-cli/build/libs/`.

## Usage
TonY is a library, so it is as simple as running a java program. First, copy your artifacts to the machine with Hadoop installed, where you plan on running TonY from. This includes:

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

    $ ls tony/
      tony-cli-0.1.0-SNAPSHOT-all.jar  tony.xml

In the `tony` directory there’s also a `tony.xml` which contains all of your TonY job configurations.
For example:

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

For a full list of configurations, please see the wiki.

#### Model code

    $ ls src/models/ | grep mnist_distributed
      mnist_distributed.py


Then you can launch your job:

    $ java -cp "`hadoop classpath --glob`:tony/*:tony" \
                com.linkedin.tony.cli.ClusterSubmitter \
                -executes src/models/mnist_distributed.py \
                -task_params '--input_dir /path/to/hdfs/input --output_dir /path/to/hdfs/output --steps 2500 --batch_size 64' \
                -python_venv my-venv.zip \
                -python_binary_path Python/bin/python \
                -src_dir src
The command line arguments are as follows:
* `executes` describes the location to the entry point of your training code.
* `task_params` describe the command line arguments which will be passed to your entry point.
* `python_venv` describes the name of the zip locally which will invoke your python script.
* `python_binary_path` describes the relative path in your python virtual environment which contains the python binary, or an absolute path to use a python binary already installed on all worker nodes.
* `src_dir` specifies the name of the root directory locally which contains all of your python model source code. This directory will be copied to all worker nodes.

#### TonY configurations

There are multiple ways to specify configurations for your TonY job. As above, you can create an XML file called `tony.xml`
and add its parent directory to your java classpath.

Alternatively, you can pass `-conf_file <name_of_conf_file>` to the java command line if you have a file not named `tony.xml`
containing your configurations. (As before, the parent directory of this file must be added to the java classpath.)

If you wish to override configurations from your configuration file via command line, you can do so by passing `-conf <tony.conf.key>=<tony.conf.value>` argument pairs on the command line.

Finally, please check `tony-default.xml` or the wiki for default values of each TonY configuration.

Here is a full example of configuring your TonY application:

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
    </configuration>

    $ java -cp "`hadoop classpath --glob`:tony/*:tony" com.linkedin.tony.cli.ClusterSubmitter \
                -task_params '--data_dir hdfs://default/data/mnist --working_dir hdfs://default/mnist/working_dir --steps 2500 --batch_size 64' \
                -python_binary_path Python/bin/python \
                -python_venv my-venv.zip \
                -executes src/mnist_distributed.py \
                -conf tony.ps.instances=2 \
                -conf tony.worker.instances=2

CLI configurations have highest priority, so we will get 2 ps instances and 2 worker instances. Then the XML file takes next priority so each worker will get 4g memory and 1 GPU. Finally every other configuration will be default value, e.g. each ps will get 2g memory.
