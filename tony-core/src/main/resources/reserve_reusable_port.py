#
# Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the
# BSD-2 Clause license. See LICENSE in the project root for license information.
#
import socket
import sys
import time
import atexit
import logging
import signal
from optparse import OptionParser


# import os # uncomment if you want to change directories within the program

def close_socket():
  print("closing port")
  s.close();
  print("port closed")

if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s')
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
  logging.warn("binding the port with SO_REUSEPORT...")
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
  # todo: teriminate if bind fails
  s.bind(("localhost", options.port))
  atexit.register(close_socket)
  signal.signal(signal.SIGTERM, close_socket)
  signal.signal(signal.SIGINT, close_socket)
  signal.signal(signal.SIGILL, close_socket)

  time.sleep(options.timeout)