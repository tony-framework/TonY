/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.jobhistory;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.log.AggregatedLogsBlock;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.*;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

public class HsLogsPage extends TwoColumnLayout {

  @Override
  protected void preHead(Page.HTML<__> html) {
    String logEntity = $(ENTITY_STRING);
    if (logEntity == null || logEntity.isEmpty()) {
      logEntity = $(CONTAINER_ID);
    }
    if (logEntity == null || logEntity.isEmpty()) {
      logEntity = "UNKNOWN";
    }
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  /**
   * The content of this page is the JobBlock
   *
   * @return HsJobBlock.class
   */
  @Override
  protected Class<? extends SubView> content() {
    return AggregatedLogsBlock.class;
  }
}
