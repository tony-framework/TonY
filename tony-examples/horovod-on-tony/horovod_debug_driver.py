#
# Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the
# BSD-2 Clause license. See LICENSE in the project root for license information.
#
import os
import logging
import time

from optparse import OptionParser
import sys
import signal
import json

try:
    import horovod.tensorflow as hvd
    from horovod.runner import gloo_run
    from horovod.runner.http.http_server import RendezvousServer
    from horovod.runner.common.util.hosts import get_host_assignments, parse_hosts
    from horovod.runner.elastic import discovery
    from horovod.runner.elastic.rendezvous import create_rendezvous_handler
    from horovod.runner.elastic.driver import ElasticDriver
except Exception as e:
    logging.error("Horovod is not installed. See README for instructions to install it")
    pass

PORT_FILE_NAME_SUFFIX = "____HOROVOD_RENDEZVOUS_SERVER____"

default_worker_list = os.getenv("CLUSTER_WORKER_LIST")
print("Print worker_list:")
print(default_worker_list)

default_output_path = os.getenv("DRIVER_OUTPUT_PATH")
print("Print output path:")
print(default_worker_list)

def elastic_driver_fn():
    pass


def static_driver_fn():
    global_rendezv = RendezvousServer(verbose=1)
    global_rendezv_port = global_rendezv.start()
    print("Rendezvous server started, port: " + str(global_rendezv_port))

    # worker_list = "localhost:1"
    hosts = parse_hosts(worker_list)
    host_alloc_plan = get_host_assignments(hosts, 1)

    global_rendezv.init(host_alloc_plan)
    return (global_rendezv_port, host_alloc_plan)

def _build_fake_host_plan():
    hostname = worker_list.split(":")[0]
    return [
        {
            "hostname": hostname,
            "rank": "0",
            "localRank": "0",
            "crossRank": "0",
            "size": "2",
            "localSize": "2",
            "crossSize": "1"
        },
        {
            "hostname": hostname,
            "rank": "1",
            "localRank": "1",
            "crossRank": "1",
            "size": "2",
            "localSize": "2",
            "crossSize": "1"
        }
    ]

def _get_host_plan_json(host_alloc_plan):
    if host_alloc_plan == None:
        return json.dumps(_build_fake_host_plan())

    hosts = []
    for plan in host_alloc_plan:
        hosts.append({
            "hostname": plan.hostname,
            "rank": plan.rank,
            "localRank": plan.local_rank,
            "crossRank": plan.cross_rank,
            "size": plan.size,
            "localSize": plan.local_size,
            "crossSize": plan.cross_size
            })
    print("Host alloc plan: \n" + json.dumps(hosts))
    return json.dumps(hosts)


def set_option():
    parser = OptionParser()
    parser.add_option(
        "-a", "--num_proc", dest="num_process", type="str", help="number process of training", default="1")
    parser.add_option(
        "-w", "--worker_list", dest="worker_list", type="str", help="worker list", default=default_worker_list
    )
    parser.add_option(
        "-e", action="store_true", help="enable elastic training.", dest="enable_elastic", default=False
    )
    parser.add_option(
        "-t", action="store_true", help="is in test mode", dest="is_in_test_mode", default=False
    )
    parser.add_option(
        "-p", "--fake_port", dest="fake_port", type="str", help="fake server port for TonY unit test"
    )
    parser.add_option(
        "-f", action="store_true", help="fast fail in test mode for TonY unit test", dest="is_fast_fail", default=False
    )
    (options, args) = parser.parse_args(sys.argv)

    global worker_list
    worker_list = options.worker_list

    global enable_elastic
    enable_elastic = options.enable_elastic
    print("Enable elastic: " + str(enable_elastic))

    global is_in_test_mode
    is_in_test_mode = options.is_in_test_mode
    global fake_server_port
    global is_fast_fail
    is_fast_fail = False
    if is_in_test_mode:
        fake_server_port = options.fake_port
        is_fast_fail = options.is_fast_fail


def __port_file_path(port):
    path_dir = default_output_path
    port_file_path = os.path.join(path_dir, str(port) + PORT_FILE_NAME_SUFFIX)
    return port_file_path


def create_port_file(port, host_alloc_plan):
    port_file = __port_file_path(port)
    logging.info("Creating port file %s", port_file)
    with open(__port_file_path(port), 'w') as fo:
        fo.write(_get_host_plan_json(host_alloc_plan))
        logging.info("Port file for %s created", port_file)
        pass


def delete_port_file(port):
    port_file = __port_file_path(port)
    logging.info("Deleting port file %s", port_file)
    try:
        os.remove(__port_file_path(port))
        logging.info("Port file %s deleted", port_file)
    except OSError:
        pass


def handle_exit(*args):
    try:
        logging.info("Closing rendezvous server...")
        # todo: Close rendezvous server.
        logging.info("Closed rendezvous server")

        delete_port_file(port)
    except:
        logging.exception("Failed to close rendezvous server")

    sys.exit(0)


if __name__ == '__main__':
    set_option()

    # Just for Unit Test
    if is_fast_fail:
        sys.exit(1)

    try:
        global port
        if enable_elastic:
            elastic_driver_fn()
        else:
            if is_in_test_mode:
                print("In unit test mode. fake port: " + fake_server_port)
                (port, host_alloc_plan) = (fake_server_port, None)
            else:
                (port, host_alloc_plan) = static_driver_fn()
            create_port_file(port, host_alloc_plan)
    except:
        logging.exception("Errors on starting horovod rendezvous server.")
        handle_exit()

    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGILL, handle_exit)
    while True:
        time.sleep(10)

