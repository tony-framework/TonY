#
# Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.
#
import time


def return_1():
    time.sleep(1)
    return 1


exit(return_1())
