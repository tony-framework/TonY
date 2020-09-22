### Running Examples
This example shows how to run a simple Ray program on TonY.

### Installing Ray dependencies
One easy way to make sure all dependencies are available is to provide a conda environment. On a Linux machine, create and activate your conda environment.
* If you don’t have conda installed, you can install it via https://docs.conda.io/en/latest/miniconda.html. Be sure to refresh your terminal.
* Create a new environment, and install ray and other dependencies.
	* conda create -n ray python=3.7
	* export PYTHONNOUSERSITE=1 (this avoids mixing the conda install with the local user install).
	* pip install -U ray 
	* pip install torch torchvision (if needed)

Use conda-pack (https://conda.github.io/conda-pack/#commandline-usage) to zip the conda environment: ```conda pack -n ray ray.zip```

### Configuration
Below is an example config file that request 1 ray head node and 2 ray worker nodes. Note that some executables within the virtual environment need to be set as executable (since their permissions are set to 640 upon unpacking). We include discovery.py script to extrat the port and set this in --redis-port for head node; we also extract the head host and port and set it in --address for worker nodes.
```
<configuration>
  <property>
    <name>tony.worker.instances</name>
    <value>2</value>
  </property>
  <property>
    <name>tony.worker.memory</name>
    <value>3g</value>
  </property>
  <property>
    <name>tony.head.instances</name>
    <value>1</value>
  </property>
  <property>
    <name>tony.head.command</name>
    <value>chmod +x $PWD/venv/lib/python3.7/site-packages/ray/core/src/ray/raylet/raylet; chmod +x $PWD/venv/lib/python3.7/site-packages/ray/core/src/ray/raylet/raylet_monitor; chmod +x $PWD/venv/lib/python3.7/site-packages/ray/core/src/plasma/plasma_store_server; chmod +x $PWD/venv/lib/python3.7/site-packages/ray/core/src/ray/thirdparty/redis/src/redis-server; chmod +x $PWD/venv/bin/*; source $PWD/venv/bin/activate; ray start --head --redis-port=`./discovery.py --head-port` --object-store-memory=200000000 --memory 200000000 --num-cpus=1; python example.py; ray stop</value>
  </property>
  <property>
      <name>tony.worker.command</name>
      <value>chmod +x $PWD/venv/lib/python3.7/site-packages/ray/core/src/ray/raylet/raylet; chmod +x $PWD/venv/lib/python3.7/site-packages/ray/core/src/ray/raylet/raylet_monitor; chmod +x $PWD/venv/lib/python3.7/site-packages/ray/core/src/plasma/plasma_store_server; chmod +x $PWD/venv/lib/python3.7/site-packages/ray/core/src/ray/thirdparty/redis/src/redis-server; chmod +x $PWD/venv/bin/*; source $PWD/venv/bin/activate; ray start --object-store-memory=200000000 --memory 200000000 --num-cpus=1 --address=`./discovery.py --head-address` --block; ray stop</value>
  </property>
</configuration>
```
