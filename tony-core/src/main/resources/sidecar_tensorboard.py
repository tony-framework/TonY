#
# Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the
# BSD-2 Clause license. See LICENSE in the project root for license information.
#
import re
import sys
import time
import os

try:
    from tensorboard.main import run_main
except Exception as e:
    print("Tensorboard is not installed. See README for instructions to install it")
    pass

test_mode = False
if 'SIDECAR_TB_TEST' in os.environ:
    test_mode = True

if __name__ == '__main__':
    if test_mode:
        print("In test mode, start waiting...")
        while True:
            time.sleep(10)
    else:
        sys.argv.append("--logdir")
        sys.argv.append(os.environ["TB_LOG_DIR"])
        sys.argv.append("--port")
        sys.argv.append(os.environ["TB_PORT"])
        sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])
        sys.exit(run_main())