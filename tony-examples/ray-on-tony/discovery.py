#!/usr/bin/env python
"""This script allows finds and passes the right address and port to Ray from the Tony configuration. 

It pulls the configuration from the environment variable and parses the json string. It assumes configuration has a string similar to {"cluster": {"head": ["host123:5343"]}}

`./discovery --head-address` should print "host123:5343".
`./discovery --head-port` should print "5343".
"""

from __future__ import print_function
import json
import os
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--head-address", 
        action="store_true",
        help="Prints the address of the head container.")
    parser.add_argument(
        "--head-port",
        action="store_true",
        help="Prints the address of the head container.")
    args = parser.parse_args()
    config_str = os.environ.get("TF_CONFIG")
    if not config_str:
        raise ValueError("Config not available.")
    config = json.loads(config_str)
    if args.head_address:
        print(config["cluster"]["head"][0])
    elif args.head_port:
        # Assume {"cluster": {"head": ["host123:123"]}}
        print(config["cluster"]["head"][0].split(":")[-1])
    else:
        raise ValueError("Not a valid command.")
