/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.jobhistory;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.*;

import com.linkedin.tony.webapp.AMParams;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.webapp.HsWebServices;
import org.apache.hadoop.mapreduce.v2.hs.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;

public class HsWebApp extends WebApp implements AMParams {

  private HistoryContext history;

  public HsWebApp(HistoryContext history) {
    this.history = history;
  }

  @Override
  public void setup() {
    bind(HsWebServices.class);
    bind(JAXBContextResolver.class);
    bind(GenericExceptionHandler.class);
    bind(AppContext.class).toInstance(history);
    bind(HistoryContext.class).toInstance(history);
    route("/", HsController.class);
    route(pajoin("/job", APP_ID), HsController.class, "job");
    route(pajoin("/logs", NM_NODENAME, CONTAINER_ID, ENTITY_STRING, APP_OWNER,
        CONTAINER_LOG_TYPE), HsController.class, "logs");
  }
}

