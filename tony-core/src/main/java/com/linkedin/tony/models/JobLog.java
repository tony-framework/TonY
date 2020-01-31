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
  private static final String LOG_URL_SCHEMA = "http://{0}/jobhistory/nmlogs/{1}:{2}/{3}/{4}/{5}";
  private static final String MAPREDUCE_JOBHISTORY_WEBAPP_ADDRESS = "mapreduce.jobhistory.webapp.address";
  private static final String YARN_NODEMANAGER_ADDRESS = "yarn.nodemanager.address";
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
   * @param e  e {@link Event}
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
      Pair<String, String> jobHistoryNodeMgrAdd = getJobHistoryNodeMgrAdd(yarnConfiguration);
      if (isParameterValid(jobHistoryNodeMgrAdd)) {
        LOG.debug("Create container log URL with following information " + " " + jobHistoryNodeMgrAdd.getLeft() + " "
            + jobHistoryNodeMgrAdd.getRight() + " " + nodeIdContainerId.getLeft() + " " + nodeIdContainerId.getRight());
        logLink = processForLogLink(jobHistoryNodeMgrAdd, nodeIdContainerId, userName);
      }
    }
    return logLink;
  }

  /**
   * @param yarnConfiguration {@link YarnConfiguration} yarnconfiguration provided to TonY
   * @return Pair contains mapreduce.jobhistory.webapp.address if able to get information from
   * yarnconf ,other will return null
   */
  private static Pair<String, String> getJobHistoryNodeMgrAdd(YarnConfiguration yarnConfiguration) {
    Pair<String, String> jobHistoryNodeMgrAddress = null;
    if (isParameterValid(yarnConfiguration)) {
      String mapReduceJobHistoryWebAddress = yarnConfiguration.get(MAPREDUCE_JOBHISTORY_WEBAPP_ADDRESS);
      String yarnNodeManagerAddress = yarnConfiguration.get(YARN_NODEMANAGER_ADDRESS);
      if (isParameterValid(mapReduceJobHistoryWebAddress, yarnNodeManagerAddress)) {
        LOG.debug("There is sufficient information to construct container url " + mapReduceJobHistoryWebAddress + " "
            + yarnNodeManagerAddress);
        jobHistoryNodeMgrAddress = Pair.of(mapReduceJobHistoryWebAddress, yarnNodeManagerAddress);
      }
    }
    return jobHistoryNodeMgrAddress;
  }

  private static boolean isParameterValid(Object... parameters) {
    return Stream.of(parameters).allMatch(Objects::nonNull);
  }

  /**
   *
   * @param jobHistoryNodeMgrAdd Job History Address and Node mgr address
   * @param nodeIdContainerId  Node ID and container Id
   * @param userName username
   * @return History server url , which will provide the container logs
   */
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
