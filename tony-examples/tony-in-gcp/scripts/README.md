# Dataproc + TonY installation scripts

## Overview

The code in this folder will help you create a Google Cloud Dataproc cluster with 2 workers and enable
TensorFlow 1.13.1 and GPU. We provide different scripts to perform functions like:

 - Create a Dataproc cluster
 - Launch jobs
 - Install TonY
 - Install GPU drivers
 - Monitor GPU utilization

## Scripts

[create_cluster](create_cluster.sh)

This script creates a single master and 2 workers cluster with 2 initialization actions: 
  - TonY
  - Install GPU Drivers

There are two kinds of calculators currently available in Hadoop 2.x: 

 - DefaultResourceCalculator 
 - DominantResourceCalculator

Dataproc by default supports `DefaultResourceCalculator`.
Dataproc supports only Hadoop 2.9.X, hence utilization of GPU is restricted to either Node Labels or by sending 
TF/PyTorch job to a node which already are using a GPU. TensorFlow by default allocates all GPU memory.
This requires one TensorFlow job per node or configuring TensorFlow to use only some percentage of the GPU.

This works for single jobs or experimentation. For distributed TensorFlow jobs please consider using Hadoop 3.x or 
plan GPU resource allocation correctly. This scripts do the following:

 - Creates a new cluster with 2 workers
 - Enables auto-scaling in cluster
 - Enables `DominantResourceCalculator` in YARN
 - Adds GPUs to each worker
 - Install TonY samples and build .jar file
 - Install GPU drivers
 - Install GPU Stackdriver monitoring service

Usage: 

```bash
./create_cluster.sh
```

If you want to restrict GPU utilization configure the following in sample `mnist_distributed.py`:

Line: 203

```
    config_proto = tf.ConfigProto(log_device_placement=False)
    config_proto.gpu_options.per_process_gpu_memory_fraction = 0.20
    server = tf.train.Server(cluster, job_name=job_name, task_index=task_index, config=config_proto)
```

**Note:** Verify you have sufficient quota for both CPU and GPU resources in your project.

[Launch jobs](launch_jobs.sh)

 - Allows sending multiple jobs to Dataproc cluster.

Usage: 

Execute 10 jobs using Tensorflow.

```bash
./launch_jobs.sh tensorflow 10
```

## Initialization actions

[tony](tony.sh)

 - Clone TonY repo
 - Build a .jar file
 - Create a sample folder for TensorFlow
 - Create a sample folder for PyTorch

#### Metadata available:

Check script file for more options. Example:

```bash
tf_version=1.13.1,tf_gpu=true
```

**Note:** You need to edit `mnist-tensorflow/mnist_distributed.py` if you want to use multiple jobs and share GPU.

[install_gpu_cuXX](install_gpu_cu90.sh)

 - Install GPU drivers
 - Install GPU Stackdriver monitoring service

We provide 2 versions:

 - TensorFlow pre 1.13 compiled using CUDA 90
 - TensorFlow 1.13+ compiled using CUDA 10

The script does the following:

 - Downloads and install GPU drivers
 - Installs CUDA driver
 - Install CuDNN drivers
 - Install NCCL drivers
 - Setup Environment variables for CUDA in Cloud Dataproc cluster

**Note:** You may need to download a different version of drivers and update your script if required.

#### Metadata available:

```bash
install_gpu_agent=true
```


## What do you want to see?

If you came looking for a sample we don’t have, please file an issue using the Sample / Feature Request template on this 
repository. 
Please provide as much detail as possible, what framework (Tensorflow, Keras, PyTorch...), the type of model, and what 
kind of dataset you were hoping to use!
