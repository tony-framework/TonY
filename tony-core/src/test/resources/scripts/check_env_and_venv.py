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

if not os.path.isfile('venv/123.xml'):
    logging.error('venv/123.xml doesn\'t exist')
    exit(-1)

if os.environ['ENV_CHECK'] == 'ENV_CHECK':
    logging.info('Found ENV_CHECK environment variable.')
    exit(0)
else:
    logging.error('Failed to find ENV_CHECK environment variable')
    exit(1)
