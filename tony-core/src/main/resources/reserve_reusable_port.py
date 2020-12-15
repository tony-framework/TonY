#
# Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the
# BSD-2 Clause license. See LICENSE in the project root for license information.
#
import socket
import sys
import time
import logging
import signal
import os
from optparse import OptionParser

PORT_FILE_NAME_SUFFIX = "___PORT___"


def __port_file_path(port):
  path_dir = os.path.dirname(os.path.abspath(__file__))
  port_file_path = os.path.join(path_dir, str(port) + PORT_FILE_NAME_SUFFIX)
  return port_file_path


def create_port_file(port):
  port_file = __port_file_path(port)
  logging.debug("Creating port file %s", port_file)
  with open(__port_file_path(port), 'w'):
    logging.debug("Port file for %s created", port_file)
    pass


def delete_port_file(port):
  port_file = __port_file_path(port)
  logging.debug("Deleting port file %s", port_file)
  try:
    os.remove(__port_file_path(port))
    logging.debug("Port file %s deleted", port_file)
  except OSError:
    pass


def handle_exit(*args):
  try:
    logging.debug("Closing port %s", options.port)
    s.close();
    logging.info("Port closed %s", options.port)
  except:
    logging.exception("Failed to close port " + options.port)

  delete_port_file(options.port)
  sys.exit(0)


if __name__ == "__main__":
  """Reserving a port with PORT_REUSE for a period of time. 
  Note it only binds to the port but do not accepts any inbound connection to 
  the port. See https://github.com/linkedin/TonY/pull/465  
  
  Following command reserves port 1024 for 3600 secs:
  python ./reserve_reusable_port -p 1024 -d 3600
  """

  logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s:%('
                             'lineno)d - %(message)s', level=logging.INFO)
  parser = OptionParser()

  parser.add_option(
      "-p", "--port", dest="port", type="int", help="port to run on")

  (options, args) = parser.parse_args(sys.argv)
  if not options.port:
    parser.error('port not given')

  global s
  try:
    logging.debug("Binding port %s with SO_REUSEPORT...", options.port)
    # binding to the port but NOT accepting any inbound connection
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    s.bind(("localhost", options.port))
    logging.debug("Port %s with SO_REUSEPORT bound...", options.port)
    create_port_file(options.port)
    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGILL, handle_exit)
  except:
    logging.exception("error in creating the socket")
    handle_exit()

  signal.pause()
  handle_exit()
