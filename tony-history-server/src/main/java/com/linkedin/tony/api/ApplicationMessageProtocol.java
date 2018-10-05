/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.api;

import org.apache.hadoop.ipc.VersionedProtocol;
import com.linkedin.tony.common.Message;

/**
 * The Protocal between clients and ApplicationMaster to fetch Application Messages.
 */
public interface ApplicationMessageProtocol extends VersionedProtocol {

  public static final long versionID = 1L;

  /**
   * Fetch application from ApplicationMaster.
   */
  Message[] fetchApplicationMessages();

  Message[] fetchApplicationMessages(int maxBatch);
}
