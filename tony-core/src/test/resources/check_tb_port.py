#
# Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.
#
import errno
import logging
import os
import socket
import sys

# Set up logging.
log_root = logging.getLogger()
log_root.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
log_root.addHandler(ch)

if 'TB_PORT' in os.environ:
    logging.info('Found TB_PORT environment variable set to ' + os.environ['TB_PORT'])

    # Check if port is accessible
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.bind(("localhost", int(os.environ['TB_PORT'])))
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            logging.error('Port ' + os.environ['TB_PORT'] + ' is already in use!')
        else:
            logging.error('Error encountered while binding to port ' + os.environ['TB_PORT'] + ': ' + str(e))
        exit(1)

    s.close()