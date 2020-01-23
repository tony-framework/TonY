/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.models;



import com.linkedin.tony.events.ApplicationInited;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.events.EventType;
import com.linkedin.tony.events.TaskStarted;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static com.linkedin.tony.Constants.*;


public class JobEvent {
  private static final Log LOG = LogFactory.getLog(JobEvent.class);
  private static final String LOG_URL_SCHEMA = "http://{0}/jobhistory/nmlogs/{1}:{2}/{3}/{4}/{5}";
  private static final String MAPREDUCE_JOBHISTORY_WEBAPP_ADDRESS = "mapreduce.jobhistory.webapp.address";
  private static final String YARN_NODEMANAGER_ADDRESS = "yarn.nodemanager.address";
  private static final String IP_ADDRESS_PORT_DELIMITER = ":";

  private enum NodeWebAddressSchema {IP_ADDRESS, PORT, MAX}



  private EventType type;
  private Object event;
  private String logLink;
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

  public String getLogLink() {
    return logLink;
  }

  public void setLogLink(String logLink) {
    this.logLink = logLink;
  }

  public static JobEvent convertEventToJobEvent(Event e, YarnConfiguration yarnConfiguration, String userName) {
    JobEvent wrapper = new JobEvent();
    wrapper.setType(e.getType());
    wrapper.setEvent(e.getEvent());
    wrapper.setTimestamp(e.getTimestamp());
    wrapper.setLogLink(createLogLink(e, yarnConfiguration, userName));
    return wrapper;
  }

  private static String createLogLink(Event e, YarnConfiguration yarnConfiguration, String userName) {
    String logLink = DEFAULT_VALUE_OF_CONTAINER_LOG_LINK;
    if (Objects.isNull(yarnConfiguration) || Objects.isNull(userName)) {
      LOG.error("Insufficient information to create log link " + yarnConfiguration + " " + userName);
      return logLink;
    }
    Pair<String, String> jobHistoryNodeMgrAdd = getJobHistoryNodeMgrAdd(yarnConfiguration);
    if (Objects.nonNull(jobHistoryNodeMgrAdd)) {
      Pair<String, String> nodeIdContainerId = processEventForNodeIdContainerId(e);
      if (Objects.nonNull(nodeIdContainerId)) {
        LOG.debug(" Create container log URL with following information " + " " + jobHistoryNodeMgrAdd.getLeft() + " "
            + jobHistoryNodeMgrAdd.getRight() + " " + nodeIdContainerId.getLeft() + " " + nodeIdContainerId.getRight());
        logLink = processForLogLink(jobHistoryNodeMgrAdd, nodeIdContainerId, userName);
      }
    }
    return logLink;
  }

  private static Pair<String, String> getJobHistoryNodeMgrAdd(YarnConfiguration yarnConfiguration) {
    String mapReduceJobHistoryWebAddress = yarnConfiguration.get(MAPREDUCE_JOBHISTORY_WEBAPP_ADDRESS);
    String yarnNodeManagerAddress = yarnConfiguration.get(YARN_NODEMANAGER_ADDRESS);
    if (mapReduceJobHistoryWebAddress != null && yarnNodeManagerAddress != null) {
      LOG.debug("There is sufficient information to construct container url " + mapReduceJobHistoryWebAddress + " "
          + yarnNodeManagerAddress);
      return Pair.of(mapReduceJobHistoryWebAddress, yarnNodeManagerAddress);
    }
    LOG.warn(" There is missing information " + mapReduceJobHistoryWebAddress + " " + yarnNodeManagerAddress);
    return null;
  }

  private static Pair<String, String> processEventForNodeIdContainerId(Event e) {
    if (e.getType().name().equals(EventType.APPLICATION_INITED.name())) {
      return Pair.of(((ApplicationInited) e.getEvent()).getHost(), ((ApplicationInited) e.getEvent()).getContainerID());
    } else if (e.getType().name().equals(EventType.TASK_STARTED.name())) {
      return Pair.of(((TaskStarted) e.getEvent()).getHost(), ((TaskStarted) e.getEvent()).getContainerID());
    }
    return null;
  }

  private static String processForLogLink(Pair<String, String> jobHistoryNodeMgrAdd,
      Pair<String, String> nodeIdContainerId, String userName) {
    String[] nodeMgrAddress = jobHistoryNodeMgrAdd.getRight().split(IP_ADDRESS_PORT_DELIMITER);
    String nodeMgrPort = null;
    if (nodeMgrAddress.length >= NodeWebAddressSchema.MAX.ordinal()) {
      nodeMgrPort = nodeMgrAddress[NodeWebAddressSchema.PORT.ordinal()];
      String logLink =
          MessageFormat.format(LOG_URL_SCHEMA, jobHistoryNodeMgrAdd.getLeft(), nodeIdContainerId.getLeft(), nodeMgrPort,
              nodeIdContainerId.getRight(), nodeIdContainerId.getRight(), userName);
      LOG.info(" Log link URL " + logLink);
      return logLink;
    }
    LOG.error(" Node mgr address is incorrect " + jobHistoryNodeMgrAdd.getRight());
    return DEFAULT_VALUE_OF_CONTAINER_LOG_LINK;
  }


}

