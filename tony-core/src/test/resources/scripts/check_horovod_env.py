#
# Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.
#
import os

controller = os.environ['HOROVOD_CONTROLLER']
operators = os.environ['HOROVOD_CPU_OPERATIONS']
timeout = os.environ['HOROVOD_GLOO_TIMEOUT_SECONDS']
rendez_port = os.environ['HOROVOD_GLOO_RENDEZVOUS_PORT']
rendez_addr = os.environ['HOROVOD_GLOO_RENDEZVOUS_ADDR']
cross_rank = os.environ['HOROVOD_CROSS_RANK']
cross_size = os.environ['HOROVOD_CROSS_SIZE']
local_rank = os.environ['HOROVOD_LOCAL_RANK']
local_size = os.environ['HOROVOD_LOCAL_SIZE']
size = os.environ['HOROVOD_SIZE']
rank = os.environ['HOROVOD_RANK']
hostname = os.environ['HOROVOD_HOSTNAME']

job_name = os.environ['JOB_NAME']

print('JOB_NAME is ' + job_name)

print('Horovod envs are as follows:')
print('controller: ' + controller)
print('operators: ' + operators)
print('timeout: ' + timeout)
print('rendez_port: ' + rendez_port)
print('rendez_addr: ' + rendez_addr)
print('cross_rank: ' + cross_rank)
print('cross_size: ' + cross_size)
print('local_rank: ' + local_rank)
print('local_size: ' + local_size)
print('size: ' + size)
print('rank: ' + rank)
print('hostname: ' + hostname)


if not (controller and job_name and operators and timeout and rendez_addr and rendez_port and cross_rank and cross_size and local_rank and local_size and size and rank):
    raise ValueError