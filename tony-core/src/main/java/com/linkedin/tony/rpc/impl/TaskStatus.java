/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl;

public enum TaskStatus {
    NEW,
    READY,
    RUNNING,
    FAILED,
    SUCCEEDED,
    FINISHED
}
