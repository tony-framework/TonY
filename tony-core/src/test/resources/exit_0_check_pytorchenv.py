"""
Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
See LICENSE in the project root for license information.
"""
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

if os.environ.get('RANK') is not None and os.environ.get('WORLD') is not None:
    logging.info('Found RANK and WORLD environment variable.')
    exit(0)
else:
    logging.error('Failed to find RANK or WORLD environment variable')
    exit(1)
