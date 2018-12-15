"""
Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
See LICENSE in the project root for license information.
"""
import time
import os

job_name = os.environ.get('JOB_NAME')
if job_name == 'chief':
    time.sleep(5)

exit(0)
