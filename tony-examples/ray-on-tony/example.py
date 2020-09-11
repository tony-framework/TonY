from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import Counter
import json
import os
import sys
import time
import ray


@ray.remote
def gethostname(x):
    # Returns the hostname of the machine where this remote function runs.
    import time
    import socket
    time.sleep(0.01)
    return x + (socket.gethostname(), )


def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        num_nodes = len(ray.nodes())
        if num_nodes < expected:
            print("{} nodes have joined so far, waiting for {} more.".format(
                num_nodes, expected - num_nodes))
            sys.stdout.flush()
            time.sleep(1)
        else:
            break


def main(num_workers):
    wait_for_nodes(num_workers + 1)

    # Check that objects can be transferred from each node to each other node.
    for i in range(10):
        print("Iteration {}".format(i))
        results = [
            gethostname.remote(gethostname.remote(())) for _ in range(100)
        ]
        print(Counter(ray.get(results)))
        sys.stdout.flush()

    print("Success!")
    sys.stdout.flush()
    time.sleep(20)


if __name__ == "__main__":
    DRIVER_MEMORY = 100 * 1024 * 1024
    # We obtain the configuration string from the TF_CONFIG env var, 
    # assuming this is being launched as a TONY job
    config_str = os.environ.get("TF_CONFIG")
    if not config_str:
        raise ValueError("Config not available.")
    config = json.loads(config_str)
    port = config["cluster"]["head"][0].split(":")[-1]
    
    # We run this script on the head node, where the Ray head is located.
    # It is important to set driver_object_store_memory or else Ray
    # will not respect the container memory constraints.
    ray.init(
        address="localhost:" + port, 
        driver_object_store_memory=DRIVER_MEMORY)
    num_workers = len(config["cluster"]["worker"])
    main(num_workers)
