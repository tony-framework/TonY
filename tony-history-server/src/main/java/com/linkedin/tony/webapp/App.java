/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.webapp;

import com.linkedin.tony.api.ApplicationContext;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

@RequestScoped
public class App {
  final ApplicationContext context;

  @Inject
  App(ApplicationContext context) {
    this.context = context;
  }
}
