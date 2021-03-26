/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause
 * license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import static org.testng.Assert.assertEquals;

import com.linkedin.tony.rpc.impl.TaskStatus;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.Test;

public class TestTaskStatus {

  @Test
  public void testTaskStatus() {
    // Verify what TaskStatus.STATUS_SORTED_BY_VISIBILITY returns is complete

    assertEquals(TaskStatus.values().length, TaskStatus.STATUS_SORTED_BY_ATTENTION.size());

    Set<TaskStatus> taskStatusSet1 = new HashSet<>(Arrays.asList(TaskStatus.values()));
    Set<TaskStatus> taskStatusSet2 = new HashSet<>(TaskStatus.STATUS_SORTED_BY_ATTENTION);

    assertEquals(taskStatusSet1, taskStatusSet2);
  }
}
