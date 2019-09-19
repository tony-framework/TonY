#!/bin/python2

import os
import datetime
import shutil
import tempfile
import re
import time
import logging

logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s', level=logging.INFO)


full_path=os.path.realpath(__file__)
base_dir = os.path.dirname( os.path.abspath(__file__) )

cwd=os.getcwd()
now=datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

task_params = [ "--param1", "--param2" ]

logging.info("base_dir={}".format(base_dir))
command = ' '.join( [
'java', '-cp', '`hadoop classpath`:{}/tony-cli/build/libs/tony-cli-0.3.21-all.jar'.format(r'/ebs/ds/hjeon/git/TonYMxNet'), 'com.linkedin.tony.cli.ClusterSubmitter',
'--python_venv=./MxNetPython3.5.zip',
'--src_dir="./src"',
'--shell_env=DMLC_USE_KUBERNETES=1',
'--shell_env=PS_VERBOSE=0',
'--conf_file=./tony_mxnet_job.xml',
'--executes=mxnet_dist_ex.py',
'--task_params="{}"'.format( ' '.join(task_params).replace('"', '\"') ),
'--python_binary_path=Python3.5/bin/python3' ] )

#'--shell_env=PS_RESEND=10',
logging.info(command) 
os.system(command)

