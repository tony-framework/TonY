/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.jobhistory;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public class HeaderBlock extends HtmlBlock {
  public HeaderBlock() {
  }

  protected void render(Block html) {
    String loggedIn = "";
    if (this.request().getRemoteUser() != null) {
      loggedIn = "Logged in as: " + this.request().getRemoteUser();
    }

    ((DIV) ((DIV) html.div("#header.ui-widget").div("#user").__(new Object[]{loggedIn}).__()).div("#logo").img("/static/xlWebApp/logo.jpg").__()).h1(this.$("title")).__();
  }
}
