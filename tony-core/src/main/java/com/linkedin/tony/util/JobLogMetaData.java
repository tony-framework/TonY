/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.util;

import org.apache.hadoop.yarn.conf.YarnConfiguration;


/**
 * This class denotes the information required to convert
 * Event into JobLog. It is pojo
 * todo: Use lambok to remove boilerplate code
 */
public class JobLogMetaData {
  private YarnConfiguration yarnConfiguration;
  private String userName;

  public JobLogMetaData(YarnConfiguration yarnConfiguration, String userName) {
    this.yarnConfiguration = yarnConfiguration;
    this.userName = userName;
  }

  public YarnConfiguration getYarnConfiguration() {
    return yarnConfiguration;
  }

  public void setYarnConfiguration(YarnConfiguration yarnConfiguration) {
    this.yarnConfiguration = yarnConfiguration;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }
}
