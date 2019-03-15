/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.client;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;

public interface StateTransitionListener {
    public void onApplicationStatusChanged(YarnApplicationState state);
}
