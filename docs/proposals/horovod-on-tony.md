- [Motivation](#motivation)
- [Goals](#goals)
- [API](#api)
- [Design](#design)
- [Usage](#usage)

## Motivation
The purpose of Horovod is to make it easy to take a single-GPU training script and successfully scale it to train across many GPUs in parallel. This has two aspects:
1. Easy to use
2. Run faster in distributed mode

However, because of the limitation of SSH mechanism (hard to support SSH on Yarn), TonY don't support horovod with **MPI controller**. With the help of **Gloo controller**, we are expecting to support horovod.

## Goals
1. Support Horovod on TonY (limited static topology support)
2. Elastic Horovod will be supported later.

## API
There are no API changes to other machine learning frameworks.

## Design
Attention: It's gloo controller that makes TonY support horovod.

From the perspective of compatibility and maintainability, it's better to directly use the [gloo_runner](https://github.com/horovod/horovod/blob/master/horovod/runner/gloo_run.py) on TonY. But after reading through Horovod's code, I find it difficult to reuse gloo_runner's code on TonY, because the existence of some unrelated codes will lead driver to start the worker through the SSH command, which is hard to be supportted on Yarn.

After having a deep understanding of Horovod code and [communicating with developers](https://github.com/horovod/horovod/discussions/2785), i know that the Gloo controller uses a rendezvous server to assign each worker role, and provides HTTP API for workers to obtain cluster information. So each worker  can build a training cluster and start training at the same time.

Horovod is served as two roles, worker and driver. Driver is responsible for starting the rendezvous server and will not participate in training (no GPU required, lightweight). Before starting, driver need to know all workers' hostnames in advance. The worker is only responsible for training. According to TonY's architecture (**Application master** and **task executor**), the design can be as follows.

### Horovod Driver
__How to start rendezvous server__  
Reusing Horovod rendezvous server code, we introduce tony-horovod driver launcher to offer a python script
```python
# Init the horovod rendezous server
global_rendezv = RendezvousServer(verbose=1)
# Output server port, which will be used horovod worker to connect server.
global_rendezv_port = global_rendezv.start()
print("Rendezvous server started, port: " + str(global_rendezv_port))

hosts = parse_hosts(worker_list)
# Output the host plan, it will output local_rank, rank and so on.
host_alloc_plan = get_host_assignments(hosts, 1)

# Start the server.
global_rendezv.init(host_alloc_plan)
```

__When to start driver__  
After all workers' resource have be assigned and TonY's Application master could get all workers' registry info.

__Where to start driver__  
Two options  
1. On TonY application master.  
This will save resources(no extra resources to start driver), and the amount of code changes will be small. But by injecting relevant Horovod's driver code into AM, it is not elegant.
2. On TonY task executor.  
Additional customization of the driver configuration is required and allow different task executors to specify different resources(because driver needn't gpu). And it is necessary to coordinate the startup sequence between the driver and other workers, because driver should start before worker. But 
we should make some changes to prevent starting in each task executor at the same time.  

First option will be adopted. The second one remains to be supported in the future.

### Horovod Worker
__where to start worker__  
Only on TonY's task executor.

__How to start worker__  
Just like start tensorflow task. 
But some envs should be injected before starting worker.

```
HOROVOD_CONTROLLER=gloo
HOROVOD_CPU_OPERATIONS=gloo
HOROVOD_GLOO_TIMEOUT_SECONDS=2000
HOROVOD_GLOO_RENDEZVOUS_PORT=9999
HOROVOD_GLOO_RENDEZVOUS_ADDR=localhost

HOROVOD_CROSS_RANK=0
HOROVOD_CROSS_SIZE=1
HOROVOD_LOCAL_RANK=0
HOROVOD_LOCAL_SIZE=1
HOROVOD_SIZE=1
HOROVOD_RANK=0
HOROVOD_HOSTNAME=192.168.0.1
```
__How to get these horovod params?__    
Acutally, these params are from  **host_alloc_plan**(mentioned in previous python code). The python script should output these params and AM will get them and assign to task executor.

## Usage
tony-test.xml is as follows, more details are shown on tony-examples module.

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
    <value>YOUR_DOCKER_IMAGE_ADDRESS</value>
  </property>
  <property>
    <name>tony.application.framework</name>
    <value>horovod</value>
  </property>
</configuration>
```