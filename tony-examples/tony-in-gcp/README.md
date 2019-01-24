![img](https://i.imgur.com/7vEjSTC.png=100x100)

## Introduction

Apache Hadoop has become an established and long-running framework for distributed storage and data processing. With Cloud Dataproc, you can set up a distributed storage platform without worrying about the underlying infrastructure. But what if you want to train TensorFlow workloads directly on your distributed data store?

This guide will explain how to install a Hadoop cluster for LinkedIn open-source project TonY (TensorFlow on YARN). You will deploy a Hadoop cluster using Cloud Dataproc and TonY to launch a distributed machine learning job. We’ll explore how you can use two of the most popular machine learning frameworks: TensorFlow and PyTorch.

TensorFlow supports distributed training, allowing portions of the model’s graph to be computed on different nodes. This distributed property can be used to split up computation to run on multiple servers in parallel. Orchestrating distributed TensorFlow is not a trivial task and not something that all data scientists and machine learning engineers have the expertise, or desire, to do—particularly since it must be done manually. TonY provides a flexible and sustainable way to bridge the gap between the analytics powers of distributed TensorFlow and the scaling powers of Hadoop. With TonY, you no longer need to configure your cluster specification manually, a task that can be tedious, especially for large clusters.
The components of our system:

### First, Apache Hadoop

Apache Hadoop is an open source software platform for distributed storage and distributed processing of very large data sets on computer clusters built from commodity hardware.  Hadoop services provides for data storage, data processing, data access, data governance, security, and operations.

### Next, Cloud Dataproc

Cloud Dataproc is a managed Spark and Hadoop service that lets you take advantage of open source data tools for batch processing, querying, streaming, and machine learning. Cloud Dataproc’s automation capability helps you create clusters quickly, manage them easily, and save money by turning clusters off when you don't need them. With less time and money spent on administration, you can focus on your jobs and your data.

### And now TonY

TonY is a framework that enables you to natively run deep learning jobs on Apache Hadoop. It currently supports TensorFlow and PyTorch. TonY enables running either single node or distributed training as a Hadoop application. This native connector, together with other TonY features, runs machine learning jobs reliably and flexibly.


## Installation

**Setup a Google Cloud Platform project**

Get started on Google Cloud Platform (GCP) by creating a new project, using the instructions found [here](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

**Create a Cloud Storage bucket**

Then create a Cloud Storage bucket. Reference [here](https://cloud.google.com/storage/docs/creating-buckets#storage-create-bucket-gsutil).

```
export BUCKET=<your bucket name>
gsutil mb gs://tony-staging
```

**Create a Hadoop cluster via Cloud Dataproc using initialization actions**

You can create your Hadoop cluster directly from Cloud Console or via an appropriate `gcloud` command. The following command initializes a cluster that consists of 1 master and 2 workers:

```
export CLUSTER_NAME=<your cluster name>
export DATAPROC_VERSION=1.3-deb9
export ZONE=us-west1-a

gcloud dataproc clusters create ${CLUSTER_NAME} --bucket ${BUCKET} \
--subnet default \
--zone $ZONE \
--master-machine-type n1-standard-4 --master-boot-disk-size 100 \
--num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 200 --image-version ${DATAPROC_VERSION} \
--initialization-actions gs://dataproc-initialization-actions/tony/tony.sh
```

When creating a Cloud Dataproc cluster, you can specify in your TonY [initialization actions](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions) script that Cloud Dataproc should run on all nodes in your Cloud Dataproc cluster immediately after the cluster is set up. 

Note: Use Cloud Dataproc version 1.3-deb9, which is supported for this deployment. Cloud Dataproc version 1.3-deb9 provides Hadoop version 2.9.0. Check this [version list](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-1.3) for details.

Once your cluster is created. You can verify that under Cloud Console > Big Data > Cloud Dataproc > Clusters, that cluster installation is completed and your cluster’s status is Running. 

Go to  Cloud Console > Big Data > Cloud Dataproc > Clusters and select your new cluster:

![img](https://i.imgur.com/7NdNYja.png)

You will see the Master and Worker nodes.

**Connect to your Cloud Dataproc master server via SSH**

Click on SSH and connect remotely to Master server.

**Verify that your YARN nodes are active**

```
yarn node -list
```

Example

```
#yarn node -list
18/11/19 00:48:23 INFO client.RMProxy: Connecting to ResourceManager at tony-staging-m/10.138.0.2:8032
18/11/19 00:48:24 INFO client.AHSProxy: Connecting to Application History server at tony-staging-m/10.138.0.2:10200
Total Nodes:2         Node-Id             Node-State Node-Http-Address       Number-of-Running-Containers
tony-staging-w-0.c.dpe-cloud-mle.internal:39349         RUNNING tony-staging-w-0.c.dpe-cloud-mle.internal:8042                             0
tony-staging-w-1.c.dpe-cloud-mle.internal:44617         RUNNING tony-staging-w-1.c.dpe-cloud-mle.internal:8042                             0
```

### Installing TonY

TonY’s Cloud Dataproc initialization action will do the following:

 - Install and build TonY from GitHub repository.
 - Create a sample folder containing TonY examples, for the following frameworks:
     - TensorFlow 
     - PyTorch

The following folders are created:

 - TonY install folder (TONY_INSTALL_FOLDER) is  located by default in:

```
/opt/tony/TonY
```

TonY samples folder (TONY_SAMPLES_FOLDER) is located by default in:

```
/opt/tony/TonY-samples
```

The Tony samples folder will provide 2 examples to run distributed machine learning jobs using:

 - TensorFlow MNIST example
 - PyTorch MNIST example


## Running a TensorFlow distributed job

**Launch a TensorFlow training job**

You will be launching the Dataproc job using a `gcloud` command.

The following folder structure was created during installation in `TONY_SAMPLES_FOLDER`, where you will find a sample Python script to run the distributed TensorFlow job.

```
.
├── tony-cli-0.1.5-all.jar
├── jobs
│   └── TFJob
│          ├── tony.xml
│          └── src
│               └── mnist_distributed.py
└── deps
    └── tf.zip
```

This is a basic MNIST model, but it serves as a good example of using TonY with distributed TensorFlow. This MNIST example uses “data parallelism,” by which you use the same model in every device, using different training samples to train the model in each device. There are many ways to specify this structure in TensorFlow, but in this case, we use “between-graph replication” using [`tf.train.replica_device_setter`](https://www.tensorflow.org/deploy/distributed).

**Dependencies**

- TensorFlow version 1.9

**Note:** If you require a more recent TensorFlow and TensorBoard version, take a look at the progress of this issue to be able to upgrade to latest TensorFlow version.


**Connect to Cloud Shell**

Open Cloud Shell via the console UI:

Use the following `gcloud` command to create a new job. Once launched, you can monitor the job. (See the section below on where to find the job monitoring dashboard in Cloud Console.)

```
export TONY_JARFILE=tony-cli-0.1.5-all.jar

gcloud dataproc jobs submit hadoop --cluster "$CLUSTER_NAME" \
--class com.linkedin.tony.cli.ClusterSubmitter \
--jars file:///opt/tony/TonY-samples/"${TONY_JARFILE}" -- \
--src_dir=/opt/tony/TonY-samples/jobs/TFJob/src \
--task_params='--data_dir /tmp/ --working_dir /tmp/' \
--conf_file=/opt/tony/TonY-samples/jobs/TFJob/tony.xml \
--executes mnist_distributed.py \
--python_venv=/opt/tony/TonY-samples/deps/tf.zip \
--python_binary_path=tf/bin/python3.5
```
## Running a PyTorch distributed job

**Launch your PyTorch training job**

For PyTorch as well, you can launch your Cloud Dataproc job using `gcloud` command.

The following folder structure was created during installation in the `TONY_SAMPLES_FOLDER`, where you will find an available sample script to run the TensorFlow distributed job:

```
.
├── tony-cli-0.1.5-all.jar
├── jobs
│   └── PTJob
│          ├── tony.xml
│          └── src
│               └── mnist_distributed.py
└── deps
    └── pytorch.zip
```

### Dependencies

 - PyTorch version 0.4
 - Torch Vision 0.2.1

### Launch a PyTorch training job

```
export TONY_JARFILE=tony-cli-0.1.5-all.jar

gcloud dataproc jobs submit hadoop --cluster "$CLUSTER_NAME" \
--class com.linkedin.tony.cli.ClusterSubmitter \
--jars file:///opt/tony/TonY-samples/"${TONY_JARFILE}" -- \
--src_dir=/opt/tony/TonY-samples/jobs/PTJob/src \
--task_params='--root /tmp/' \
--conf_file=/opt/tony/TonY-samples/jobs/PTJob/tony.xml \
--executes mnist_distributed.py \
--python_venv=/opt/tony/TonY-samples/deps/pytorch.zip \
--python_binary_path=pytorch/bin/python3.5
```

## Verify Job run successfully

You can track Job status from the Dataproc Jobs tab: navigate to Cloud Console > Big Data > Dataproc > Jobs.

![img](https://i.imgur.com/OJL5Ook.png)

**Access Hadoop UI**

Logging via web to Cloud Dataproc’s master node via web: http://<Node_IP>:8088 and track Job status. Please take a look at this [section](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces) to see how to access the Cloud Dataproc UI.

You can also track Job status from DataProc Jobs tab: Cloud Console -> Big Data -> DataProc -> Jobs

![img](https://i.imgur.com/3XgZI5Z.png)


## Conclusion
Deploying TensorFlow on YARN enables you to train models straight from your data infrastructure that lives in HDFS and Cloud Storage.


#### Limitations

 - DataProc supports GPU but this has not been tested.
 - Dataproc only supports 2.X. Hadoop version 3 implements GPU isolations.

#### Troubleshooting

**Check Job status in DataProc**

![img](https://i.imgur.com/vj4Gaeo.png)

Check Job status in Hadoop UI

![img](https://i.imgur.com/sTDWjRa.png)

**Collect logs**

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
http://<Node IP>:8042/logs/userlogs/application_1542587994073_0013/
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
