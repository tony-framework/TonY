/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.models;

import com.linkedin.tony.events.Event;
import com.linkedin.tony.events.EventType;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.linkedin.tony.Constants.LOGS_SUFFIX;


public class JobEvent {
  private static final Log LOG = LogFactory.getLog(JobEvent.class);

  private String jobLogsLink;
  private EventType type;
  private Object event;
  private long timestamp;

  public EventType getType() {
    return type;
  }

  public Object getEvent() {
    return event;
  }

  public Date getTimestamp() {
    return new Date(timestamp);
  }

  public void setType(EventType type) {
    this.type = type;
  }

  public void setEvent(Object event) {
    this.event = event;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getLogsLink() {
    return jobLogsLink;
  }

  public void setLogsLink(String jobLogsLink) {
    this.jobLogsLink = jobLogsLink;
  }

  public static JobEvent convertEventToJobEvent(Event e, String jobID) {
    JobEvent wrapper = new JobEvent();
    wrapper.setType(e.getType());
    wrapper.setEvent(e.getEvent());
    wrapper.setTimestamp(e.getTimestamp());
    wrapper.setLogsLink("/" + LOGS_SUFFIX + "/" + jobID);
    return wrapper;
  }
}

