/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.webapp;

import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public class NavBlock extends HtmlBlock {

  @Override
  protected void render(Block html) {
    html.
        div("#nav").
        h3("Tools").
        ul().
        li().a("/conf", "Configuration").__().
        li().a("/stacks", "Thread dump").__().
        li().a("/logs", "Logs").__().
        li().a("/jmx?qry=Hadoop:*", "Metrics").__().__().__();
  }
}
