![img](https://i.imgur.com/7vEjSTC.png=100x100)

## Introduction

Cloud [Dataproc](https://cloud.google.com/dataproc/) is a managed Spark and Hadoop service that lets you take advantage of open source data tools for batch
processing, querying, streaming, and machine learning. Cloud Dataproc automation helps you create clusters quickly, 
manage them easily, and save money by turning clusters off when you don't need them. With less time and money spent on 
administration, you can focus on your jobs and your data.

The goal of this document is to provide the instructions required to install a Hadoop cluster for LinkedIn Open source 
project [TonY](https://github.com/linkedin/TonY/) (TensorFlow on YARN). This tutorial will launch a distributed [TensorFlow](https://www.tensorflow.org/) job. 

### What is Apache Hadoop?

[Apache Hadoop](http://hadoop.apache.org/) is an open source software platform for distributed storage and distributed processing of very large 
datasets on computer clusters built from commodity hardware.  Hadoop services provides for data storage, data processing, 
data access, data governance, security, and operations

### What is TonY?

TonY is a framework to natively run deep learning jobs on Apache Hadoop. It currently supports [TensorFlow](https://www.tensorflow.org/) and [PyTorch](https://pytorch.org/). 
TonY enables running either single node or distributed training as a Hadoop application. This native connector, 
together with other TonY features, aims to run machine learning jobs reliably and flexibly.

## Installation

**Setup a Google Cloud Platform project**

Get started into GCP by generating a new Project. Start [here](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

Create a GCS bucket. Reference [here](https://cloud.google.com/storage/docs/creating-buckets#storage-create-bucket-gsutil).

```
gsutil mb gs://tony-staging
```

**Create a Hadoop cluster in GCP via DataProc**

You can create Hadoop cluster directly from Google Cloud Console or via gcloud command:

The following command installs 2 workers:

```
gcloud dataproc clusters create tony-staging --bucket tony-staging --subnet default --zone us-west1-a \
--master-machine-type n1-standard-4 --master-boot-disk-size 200 --num-workers 2 --worker-machine-type n1-standard-4 \
--worker-boot-disk-size 500 --image-version 1.3-deb9 --project dpe-cloud-mle
```

Once Cluster is created. You can verify it under Cloud Console -> Big Data -> DataProc -> Clusters that your cluster is being created. Example:

```
Waiting on operation [projects/dpe-cloud-mle/regions/global/operations/43ef2536-0e73-37a2-9470-20d4a4fb9883].Waiting for cluster creation operation...done.Created [https://dataproc.googleapis.com/v1/projects/dpe-cloud-mle/regions/global/clusters/tony-staging] Cluster placed in zone [us-west1-a].
```

Scaling cluster (Optional)

If you want to increase the number of workers after initial installation, you can run:

```
gcloud dataproc clusters update tony-staging --num-workers 4
```

#### Connect via SSH

Go to  Cloud Console -> DataProc -> Big Data -> Clusters 

![img](https://i.imgur.com/7NdNYja.png)

**Connect SSH to DataProc master server:**

Click on SSH and connect remotely to Master server.

**Verify Operating System version:**

Google Cloud Dataproc installs a Debian version 9. This can be modified via `image-version` parameter.

```
uname -a
Linux tony-staging-m 4.9.0-8-amd64 #1 SMP Debian 4.9.110-3+deb9u6 (2018-10-08) x86_64 GNU/Linux
```
```
cat /etc/debian_version
9.5
```
```
cat /etc/issue
Debian GNU/Linux 9 \n \l
```
```
cat /etc/os-release
PRETTY_NAME="Debian GNU/Linux 9 (stretch)"
NAME="Debian GNU/Linux"
VERSION_ID="9"
VERSION="9 (stretch)"
ID=debian
HOME_URL="https://www.debian.org/"SUPPORT_URL="https://www.debian.org/support"
BUG_REPORT_URL="https://bugs.debian.org/"
```

**Verify Hadoop, HDFS and YARN versions**

```
hadoop version
```

Example output

```
Hadoop 2.9.0 Subversion 
https://bigdataoss-internal.googlesource.com/third_party/apache/hadoop -r e8ce80c37eebb173fc688e7f5686d7df74d182aa
Compiled by bigtop on 2018-10-25T12:56Z
Compiled with protoc 2.5.0From source with checksum 1eb388d554db8e1cadcab4c1326ee72
This command was run using /usr/lib/hadoop/hadoop-common-2.9.0.jar
```

**Verify Java installation**

Java version 1.8, is installed by default. In order to verify you have Java version > 1.8 run:

```
java -version
```

Example:

```
openjdk version "1.8.0_171"OpenJDK Runtime Environment (build 1.8.0_171-8u171-b11-1~bpo8+1-b11)
OpenJDK 64-Bit Server VM (build 25.171-b11, mixed mode))
```

Verify JAVA jdk:

```
echo $JAVA_HOME
```

Verify both are matching JAVA 1.8, by default $JAVA_HOME is set to version 1.8, if not, change it to Java version 1.8 using:

```
sudo update-alternatives --config java
```

Verify YARN nodes are active

```
yarn node -list
```

Example

```
yarn node -list

18/11/19 00:48:23 INFO client.RMProxy: Connecting to ResourceManager at tony-staging-m/10.138.0.2:8032
18/11/19 00:48:24 INFO client.AHSProxy: Connecting to Application History server at tony-staging-m/10.138.0.2:10200
Total Nodes:2         Node-Id             Node-State Node-Http-Address       Number-of-Running-Containers
tony-staging-w-0.c.dpe-cloud-mle.internal:39349         RUNNING tony-staging-w-0.c.dpe-cloud-mle.internal:8042                             0
tony-staging-w-1.c.dpe-cloud-mle.internal:44617         RUNNING tony-staging-w-1.c.dpe-cloud-mle.internal:8042                             0
```

Connect to Cloud Shell

Open cloud shell via Pantheon UI

![img](https://i.imgur.com/3FIIk5Y.png)

**Verify PySpark**

Submit a PySpark to verify installation.

```
gcloud dataproc jobs submit pyspark --cluster tony-staging --region global \
gs://dataproc-examples-2f10d78d114f6aaec76462e3c310f31f/src/pyspark/hello-world/hello-world.py
```

**Verify MapReduce job (Optional)**

Instructions [here](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples/tree/master/java/dataproc-wordcount)

#### Connect SSH to DataProc master server

Click on SSH and connect remotely to master server. 

### Installing TonY

Run the following commands in the DataProc master server.

**Clone Tony repository**

```
cd /usr/local/src
sudo git clone https://github.com/linkedin/TonY.git
```

**Build Tony**

```
cd TonY
sudo -E ./gradlew build
```

**Build Tony (Excluding tests)(Optional)**

```
sudo -E ./gradlew build -x test
```

**Verify Target**

Verify that tony-cli-0.1.5-all.jar file is created.

```
ls -alh tony-cli/build/libs/
total 36M
drwxr-sr-x  2 root staff 4.0K Nov 19 01:05 .
drwxr-sr-x 10 root staff 4.0K Nov 19 01:05 ..
-rw-r--r--  1 root staff  36M Nov 19 01:05 tony-cli-0.1.5-all.jar
-rw-r--r--  1 root staff 9.6K Nov 19 01:05 tony-cli-0.1.5.jar
```

**Edit yarn-site.xml**

There is a bug in DataProc installation which adds an extra ‘ which causes TonY TensorFlow jobs to fail.

1. Open yarn-site.xml 

```
sudo vim /etc/hadoop/conf.empty/yarn-site.xml
```

2. Remove extra `‘` under `yarn.application.classpath`

```
<property>
    <name>yarn.application.classpath</name>
    <value>$HADOOP_CONF_DIR,
      $HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,
      $HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,
      $HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,
      $HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*'</value>
</property>
```

#### Running a TensorFlow distributed job

**Create a virtual environment**

Create a Python 3 Virtual environment*

```
mkdir -p /usr/local/src/deps
cd /usr/local/src/deps
sudo virtualenv -p python3 tf19
```

*TensorFlow requires  Python 3.4, 3.5 or 3.6

**Install TensorFlow**

Install [TensorFlow](https://www.tensorflow.org/) 1.9. You need to install it as root to avoid issue with six library permissions. 
(If you want to run a more recent TensorFlow versions, take a look at this [issue](https://github.com/linkedin/TonY/issues/42) 
with TensorBoard)

```
sudo -i
cd /usr/local/src/deps
source tf19/bin/activate
python -V
pip install tensorflow==1.9
Compress virtual environment by creating a ZIP file
zip -r tf19.zip tf19
```

**Prepare a TonY job**

Create a folder with the required code to launch a TonY TensorFlow job.
The following folder structure is suggested:

```
.
├── src
│   └── mnist_distributed.py
├── tony-cli-0.1.5-all.jar
├── tony.xml
└── env
    └── tf19.zip
```

Create a folder to store the required files:

```
mkdir -p /usr/local/src/jobs/TFJob/src
mkdir -p /usr/local/src/jobs/TFJob/env
```

Copy `mnist_distributed.py` to `TFJob/src` folder:

```
cp /usr/local/src/TonY/tony-examples/mnist-tensorflow/mnist_distributed.py /usr/local/src/jobs/TFJob/src
```

Create a TonY configuration file

```
cd /usr/local/src/jobs/TFJob/
vim tony.xml
```
Copy the contents and save the file:

```
<configuration>
  <property>
    <name>tony.application.security.enabled</name>
   <value>false</value>
  </property>    
  <property>
    <name>tony.worker.instances</name>
    <value>2</value>
  </property>
  <property>
    <name>tony.worker.memory</name>
    <value>4g</value>
  </property>
  <property>
    <name>tony.worker.gpus</name>
    <value>0</value>
  </property>
  <property>
    <name>tony.ps.memory</name>
    <value>4g</value>
  </property>
</configuration>
```

Full file configuration details [here](https://github.com/linkedin/TonY/wiki/TonY-Configurations).

Copy virtual environment to job folder

```
cp /usr/local/src/deps/tf19.zip /usr/local/src/jobs/TFJob/env/
```

Copy `tony-cli-0.1.5-all.jar` to TFJob folder.

```
cp /usr/local/src/TonY/tony-cli/build/libs/tony-cli-0.1.5-all.jar /usr/local/src/jobs/TFJob/
```

Copy TensorFlow environment and `mnist_distributed.py` to your GCS bucket.

We need to distribute these files to all workers in Cluster.

Create a folder called tensorflow (In gsutil just pass tensorflow reference, no need to create directory)

```
cd /usr/local/src/jobs/TFJob/
gsutil cp tony-cli-0.1.5-all.jar gs://tony-staging/
gsutil cp env/tf19.zip gs://tony-staging/tensorflow/
gsutil cp src/mnist_distributed.py gs://tony-staging/tensorflow/
```

Update DataProc workers

Connect via SSH to each of the DataProc workers. Access VM instances via SSH

In each of the workers create the following directory structure. 
Create a folder to store the required files: 

```
sudo mkdir -p /usr/local/src/jobs/TFJob/src

cd /usr/local/src/jobs/TFJob/
sudo gsutil cp gs://tony-staging/tensorflow/tf19.zip env/
sudo gsutil cp gs://tony-staging/tensorflow/mnist_distributed.py src/
```

Create output folders

Create temporary folders for TensorFlow job in each worker

```
mkdir -p /tmp/data
mkdir -p /tmp/output
chmod 777 /tmp/data
chmod 777 /tmp/output
```

Launch training job

```
gcloud dataproc jobs submit hadoop --cluster tony-staging \
--class com.linkedin.tony.cli.ClusterSubmitter \
--jars file:///usr/local/src/jobs/TFJob/tony-cli-0.1.5-all.jar -- \
--python_venv=/usr/local/src/jobs/TFJob/env/tf19.zip \
--src_dir=/usr/local/src/jobs/TFJob/src \
--executes=/usr/local/src/jobs/TFJob/src/mnist_distributed.py \
--task_params='--data_dir /tmp/data/ --working_dir /tmp/output' \
--conf_file=/usr/local/src/jobs/TFJob/tony.xml \
--python_binary_path=tf19/bin/python3.5

```

Launch training job using GCS paths

```
gcloud dataproc jobs submit hadoop --cluster tony-staging \
--class com.linkedin.tony.cli.ClusterSubmitter \
--jars gs://tony-staging/tony-cli-0.1.5-all.jar -- \
--src_dir=/usr/local/src/jobs/TFJob/src \
--task_params='--data_dir /tmp/data/ --working_dir /tmp/output' \
--conf_file=/usr/local/src/jobs/TFJob/tony.xml \
--conf tony.worker.resources=gs://tony-staging/tensorflow/ \
--conf tony.ps.resources=gs://tony-staging/tensorflow/ \
--executes 'unzip tf19.zip && tf19/bin/python3.5 mnist_distributed.py'
```

### Verify Job run successfully

Access Hadoop UI

Logging via web to DataProc master node via web: http://<Node_IP>:8088 and track Job status. If you don’t have access to UI, please jump into the Create a Firewall rule section.

You can also track Job status from DataProc Jobs tab: Cloud Console -> Big Data -> DataProc -> Jobs

Create a Firewall rule

VPC network -> Create Firewall Rule

```
Targets: All instances in the network
Source IP ranges: Your IP address/network
Protocols and ports: TCP 8042 , TCP 8088
```

```
gcloud compute --project=<Your Project> firewall-rules create yarn --direction=INGRESS --priority=1000 --network=default --action=ALLOW --rules=tcp:8042,tcp:8088 --source-ranges=<Your IP Address/Subnet>
```

Since DataProc servers were allocated a Public IP address, we will be able to access Master Node and workers Hadoop web page to verify job executed successfully.

#### Limitations

 - DataProc does not support GPU. 
 - Dataproc only supports 2.X. Hadoop version 3 implements GPU isolations.
 - Workaround: Install Hadoop from scratch with Hadoop 3.X, (Not recommended)

#### Appendix

**Troubleshooting**

Check Job status in DataProc

Check Job status in Hadoop UI

Collect logs

Click on the Application running

Collect logs from each worker node by clicking in each of the containers. Logs to collect:

- amstderr.log
- amstdout.log
- prelaunch.err
- prelaunch.out

You can also identify the node which launch the process and get the logs via:

```
http:/<Node_IP>:8042/logs/userlogs/<App_ID>/
```

Example

```
http://35.233.187.222:8042/logs/userlogs/application_1542587994073_0013/
```

Check Application status

```
yarn application -list -appStates ALL

18/11/19 07:59:27 INFO client.RMProxy: Connecting to ResourceManager at tony-staging-m/10.138.0.2:8032
18/11/19 07:59:27 INFO client.AHSProxy: Connecting to Application History server at tony-staging-m/10.138.0.2:10200Total number of applications (application-types: [], states: [NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED] and tags: []):6                

Application-Id      Application-Name        Application-Type          User           Queue                   State             Final-State             Progress                       Tracking-URL
application_1542587994073_0011  TensorFlowApplication             TENSORFLOW          root         default                FINISHED               SUCCEEDED                 100%                                N/A
application_1542587994073_0012  TensorFlowApplication             TENSORFLOW          root         default                FINISHED               SUCCEEDED                 100%                                N/A
application_1542587994073_0009  TensorFlowApplication             TENSORFLOW          root         default                  KILLED                  KILLED                 100% http://tony-staging-m:8188/applicationhistory/app/application_1542587994073_0009
application_1542587994073_0010  TensorFlowApplication             TENSORFLOW          root         default                FINISHED                  FAILED                 100%                                N/Aapplication_1542587994073_0001

```

Kill application

```
yarn application -kill <application id>
```

**Connect to UI**

Firewall rules

Create a Firewall rule with TCP ports 8042 and 8088 for each of the VM instances.
Connect via Web to the Public IP address of each DataProc instance.  Example:

```
gcloud compute --project=tony-gcp firewall-rules create yarn --direction=INGRESS --priority=1000 --network=default --action=ALLOW --rules=tcp:8042,tcp:8088 --source-ranges=172.16.1.1
```
