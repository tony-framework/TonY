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

files = os.listdir("./")
for name in files:
    logging.info(name)
if not os.path.isfile('./common.zip'):
    logging.error('common.zip doesn\'t exist')
    exit(-1)
if not os.path.isfile('./test20.zip'):
    logging.error('test20.zip doesn\'t exist')
    exit(-1)
if not os.path.isfile('./test2.zip/123.xml'):
    logging.error('123.xml doesn\'t exist')
    exit(-1)
