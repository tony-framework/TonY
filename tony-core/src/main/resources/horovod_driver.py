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
except (ModuleNotFoundError, ImportError) as e:
    logging.warn("Horovod is not installed. See README for instructions to install it")

PORT_FILE_NAME_SUFFIX = "____HOROVOD_RENDEZVOUS_SERVER____"

FAKE_PORT_IN_TEST_MODE = 9999

def _elastic_driver_fn():
    global_rendezv = RendezvousServer(verbose=1)
    discover_hosts = discovery.HostDiscoveryScript("/Users/zuston/iqiyiDev/horovod-opal/dis.sh", 3)
    driver = ElasticDriver(global_rendezv, discover_hosts, min_np=2, max_np=4)
    handler = create_rendezvous_handler(driver)
    global_rendezv_port = global_rendezv.start(handler)
    print('port: ' + str(global_rendezv_port))
    print('wait for available slots: {}'.format(2))
    current_hosts = driver.wait_for_available_slots(2)
    print("current hosts:" + str(current_hosts))
    pending_slots = driver._update_host_assignments(current_hosts)
    print("pending hosts:" + str(pending_slots))
    driver._worker_registry.reset(driver.world_size())

def _static_driver_fn():
    global_rendezv = RendezvousServer(verbose=1)
    global_rendezv_port = global_rendezv.start()
    print("rendezvous server started. port: " + str(global_rendezv_port))

    # worker_list = "localhost:1"
    hosts = parse_hosts(worker_list)
    host_alloc_plan = get_host_assignments(hosts, 1)
    print(host_alloc_plan)

    global_rendezv.init(host_alloc_plan)
    return (global_rendezv_port, host_alloc_plan)

def _get_host_plan_json(host_alloc_plan):
    hosts = []
    for plan in host_alloc_plan:
        hosts.append({
            "hostname": plan.hostname,
            "rank": plan.rank,
            "local_rank": plan.local_rank,
            "cross_rank": plan.cross_rank,
            "size": plan.size,
            "local_size": plan.local_size,
            "cross_size": plan.cross_size
            })
    print(json.dumps(hosts))
    return json.dumps(hosts)

def _setOption():
    parser = OptionParser()
    parser.add_option(
        "-a", "--num_proc", dest="num_process", type="str", help="number process of training", default="1")
    parser.add_option(
        "-w", "--worker_list", dest="worker_list", type="str", help="worker list"
    )
    parser.add_option(
        "-e", action="store_true", help="enable elastic training.", dest="enable_elastic", default=False
    )
    parser.add_option(
        "-t", action="store_false", help="is in test mode", dest="is_in_test_mode", default=False
    )
    (options, args) = parser.parse_args(sys.argv)

    global worker_list
    worker_list = options.worker_list
    global enable_elastic
    enable_elastic = options.enable_elastic
    print("enable elastic:" + str(enable_elastic))
    global is_in_test_mode
    is_in_test_mode = options.is_in_test_mode

def __port_file_path(port):
    path_dir = os.path.dirname(os.path.abspath(__file__))
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
    try:
        _setOption()
        global port
        if enable_elastic:
            _elastic_driver_fn()
        else:
            (port, host_alloc_plan) = _static_driver_fn()
            create_port_file(port, host_alloc_plan)
            signal.signal(signal.SIGTERM, handle_exit)
            signal.signal(signal.SIGINT, handle_exit)
            signal.signal(signal.SIGILL, handle_exit)
    except:
        logging.exception("errors on staring horovod rendezvous server")
        handle_exit()

    time.sleep(2000)
    handle_exit()