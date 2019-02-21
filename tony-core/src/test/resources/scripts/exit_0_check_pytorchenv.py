#
# Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.
#
import time
import logging
import os
import sys

time.sleep(1)

# Set up logging.
log_root = logging.getLogger()
log_root.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
log_root.addHandler(ch)

if os.environ.get('RANK') is None:
    logging.error('Failed to find RANK environment variable')
    exit(1)
if os.environ.get('WORLD') is None:
    logging.error('Failed to find WORLD environment variable')
    exit(1)
if os.environ.get('INIT_METHOD') is None:
    logging.error('Failed to find INIT_METHOD environment variable')
    exit(1)

exit(0)
