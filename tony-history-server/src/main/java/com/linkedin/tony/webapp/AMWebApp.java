/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.webapp;

import org.apache.hadoop.yarn.webapp.WebApp;

public class AMWebApp extends WebApp implements AMParams {

  @Override
  public void setup() {
    route("/", AppController.class);
    route("/savedmodel", AppController.class, "savedmodel");
  }
}
