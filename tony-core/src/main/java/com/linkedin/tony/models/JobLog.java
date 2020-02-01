/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.models;

import com.linkedin.tony.events.ApplicationInited;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.events.TaskStarted;
import com.linkedin.tony.util.JobLogMetaData;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static com.linkedin.tony.Constants.DEFAULT_VALUE_OF_CONTAINER_LOG_LINK;


/**
 * JobLog is responsible for parsing the event and create the information
 * about container logs .
 */
public class JobLog {
  private static final Log LOG = LogFactory.getLog(JobLog.class);
  private static final String LOG_URL_SCHEMA = "http://{0}:{1}/node/containerlogs/{2}/{3}";
  private static final String YARN_NODEMANAGER_WEBAPP_ADDRESS = "yarn.nodemanager.webapp.address";
  private static final String IP_ADDRESS_PORT_DELIMITER = ":";

  private enum NodeWebAddressSchema { IP_ADDRESS, PORT, MAX }

  private String hostAddress;
  private String containerID;
  private String logLink;

  public String getHostAddress() {
    return hostAddress;
  }

  public void setHostAddress(String hostAddress) {
    this.hostAddress = hostAddress;
  }

  public String getContainerID() {
    return containerID;
  }

  public void setContainerID(String containerID) {
    this.containerID = containerID;
  }

  public String getLogLink() {
    return logLink;
  }

  public void setLogLink(String logLink) {
    this.logLink = logLink;
  }

  /**
   *
   * @param e {@link Event}
   * @param jobLogMetaData : Metadata about Job , which is required to create JobLog
   * @return JobLog
   */
  public static JobLog convertEventToJobLog(Event e, JobLogMetaData jobLogMetaData) {
    JobLog wrapper = new JobLog();
    Pair<String, String> nodeIdContainerId = processEventForNodeIdContainerId(e);
    if (isParameterValid(nodeIdContainerId)) {
      wrapper.setContainerID(nodeIdContainerId.getRight());
      wrapper.setHostAddress(nodeIdContainerId.getLeft());
      wrapper.setLogLink(
          createLogLink(nodeIdContainerId, jobLogMetaData.getYarnConfiguration(), jobLogMetaData.getUserName()));
    }
    return wrapper;
  }

  /**
   *
   * @param e {@link Event}
   * @return Pair contains node host address and container id if desired  event type is there
   * return null is other events there
   */
  private static Pair<String, String> processEventForNodeIdContainerId(Event e) {
    Pair<String, String> nodeIDContainerID = null;
    switch (e.getType()) {
      case APPLICATION_INITED:
        nodeIDContainerID =
            Pair.of(((ApplicationInited) e.getEvent()).getHost(), ((ApplicationInited) e.getEvent()).getContainerID());
        break;
      case TASK_STARTED:
        nodeIDContainerID =
            Pair.of(((TaskStarted) e.getEvent()).getHost(), ((TaskStarted) e.getEvent()).getContainerID());
        break;
      default:
        LOG.debug(" Event type doesnt have node id  and container id information " + e.getType().name());
        break;
    }
    return nodeIDContainerID;
  }

  /**
   * @param nodeIdContainerId: Node Id and container Id information
   * @param yarnConfiguration Yarnconfiguration provided to TonY
   * @param userName username who launched the job
   * @return LogLink , it will be NA for TASK_FINISHED and APPLICATION_FINISHED event
   * APPLICATION_INITED, TASK_STARTED value which will be jobhistory URL to see the container logs
   */
  private static String createLogLink(Pair<String, String> nodeIdContainerId, YarnConfiguration yarnConfiguration,
      String userName) {
    String logLink = DEFAULT_VALUE_OF_CONTAINER_LOG_LINK;
    if (isParameterValid(userName)) {
      LOG.debug(" Valid values in yarn configuration and username");
      String nodeManagerWebAppAddress = getNodeMgrWebAppAddress(yarnConfiguration);
      if (isParameterValid(nodeManagerWebAppAddress)) {
        LOG.debug("Create container log URL with following information " + " " + nodeManagerWebAppAddress + " "
            + nodeIdContainerId.getLeft() + " " + nodeIdContainerId.getRight());
        logLink = processForLogLink(nodeManagerWebAppAddress, nodeIdContainerId, userName);
      }
    }
    return logLink;
  }

  /**
   * @param yarnConfiguration {@link YarnConfiguration} yarnconfiguration provided to TonY
   * @return NodeMgrWebAppAddress if able to get information from
   * yarnconf ,other will return null
   */
  private static String getNodeMgrWebAppAddress(YarnConfiguration yarnConfiguration) {
    String yarnNodeManagerWebAppAddress = null;
    if (isParameterValid(yarnConfiguration)) {
      yarnNodeManagerWebAppAddress = yarnConfiguration.get(YARN_NODEMANAGER_WEBAPP_ADDRESS);
      if (isParameterValid(yarnNodeManagerWebAppAddress)) {
        LOG.debug("There is sufficient information to construct container url " + yarnNodeManagerWebAppAddress);
      }
    }
    return yarnNodeManagerWebAppAddress;
  }

  private static boolean isParameterValid(Object... parameters) {
    return Stream.of(parameters).allMatch(Objects::nonNull);
  }

  /**
   *
   * @param nodeManagerWebAppAddress WebApp address of the node mgr
   * @param nodeIdContainerId  Node ID and container Id
   * @param userName username
   * @return Node Manager URL for container logs
   */
  private static String processForLogLink(String nodeManagerWebAppAddress, Pair<String, String> nodeIdContainerId,
      String userName) {
    String[] nodeMgrAddress = nodeManagerWebAppAddress.split(IP_ADDRESS_PORT_DELIMITER);
    String nodeMgrPort;
    if (nodeMgrAddress.length >= NodeWebAddressSchema.MAX.ordinal()) {
      nodeMgrPort = nodeMgrAddress[NodeWebAddressSchema.PORT.ordinal()];
      String logLink =
          MessageFormat.format(LOG_URL_SCHEMA, nodeIdContainerId.getLeft(), nodeMgrPort, nodeIdContainerId.getRight(),
              userName);
      LOG.info(" Log link URL " + logLink);
      return logLink;
    }
    LOG.error(" Node mgr address is incorrect " + nodeManagerWebAppAddress);
    return DEFAULT_VALUE_OF_CONTAINER_LOG_LINK;
  }
}
