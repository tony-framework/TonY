#
# Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the
# BSD-2 Clause license. See LICENSE in the project root for license information.
#
import socket
import sys
import time
import logging
import signal
from optparse import OptionParser

def close_socket(*args):
  logging.info("closing port %s...", options.port)
  s.close();
  logging.info("port closed %s...", options.port)
  sys.exit(0)

if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s:%('
                             'lineno)d - %(message)s',
                      level=logging.INFO)
  parser = OptionParser()

  parser.add_option(
      "-p", "--port", dest="port", type="int", help="port to run on")
  parser.add_option(
      "-t", "--timeout", dest="timeout", type="int", help="timeout")

  (options, args) = parser.parse_args(sys.argv)
  if not options.port:
    parser.error('port not given')
  if not options.timeout:
    parser.error('timeout not given')

  global s
  try:
    logging.info("binding port %s with SO_REUSEPORT...", options.port)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    signal.signal(signal.SIGTERM, close_socket)
    signal.signal(signal.SIGINT, close_socket)
    signal.signal(signal.SIGILL, close_socket)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    s.bind(("localhost", options.port))
    logging.info("port %s with SO_REUSEPORT binded...", options.port)
  except:
    logging.exception("error in creating the socket")
    close_socket()
    sys.exit(1)

  logging.info("sleeping for %s sec(s)...", options.timeout)
  time.sleep(options.timeout)
  close_socket()
  sys.exit(0)
