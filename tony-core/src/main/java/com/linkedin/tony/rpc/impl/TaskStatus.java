/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl;

import com.google.common.collect.ImmutableList;

public enum TaskStatus {
    NEW,
    READY,
    RUNNING,
    FAILED,
    SUCCEEDED,
    FINISHED; // for untracked tasks killed by the AM
    // If new status is added, please also add it to {@link STATUS_SORTED_BY_VISIBILITY}

    // Ordered status from most attention-worthy to least
    public static final ImmutableList<TaskStatus> STATUS_SORTED_BY_ATTENTION =
        ImmutableList.of(FAILED, SUCCEEDED, FINISHED, RUNNING, NEW, READY);
}
