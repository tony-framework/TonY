/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface THSConstants {

  String THS_JOB_CONFIGURATION = "core-site.xml";

  String THS_APPLICATION_JAR = "AppMaster.jar";

  String WORKER = "worker";

  String PS = "ps";

  String STREAM_INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";

  String STREAM_OUTPUT_DIR = "mapreduce.output.fileoutputformat.outputdir";

  enum Environment {
    HADOOP_USER_NAME("HADOOP_USER_NAME"),

    THS_APP_TYPE("THS_APP_TYPE"),

    THS_APP_NAME("THS_APP_NAME"),

    THS_CONTAINER_MAX_MEMORY("THS_MAX_MEM"),

    THS_LIBJARS_LOCATION("THS_LIBJARS_LOCATION"),

    THS_TF_ROLE("TF_ROLE"),

    THS_TF_INDEX("TF_INDEX"),

    THS_TF_CLUSTER_DEF("TF_CLUSTER_DEF"),

    THS_MXNET_WORKER_NUM("DMLC_NUM_WORKER"),

    THS_MXNET_SERVER_NUM("DMLC_NUM_SERVER"),

    THS_LIGHTGBM_WORKER_NUM("LIGHTGBM_NUM_WORKER"),

    THS_LIGHTLDA_WORKER_NUM("LIGHTLDA_NUM_WORKER"),

    THS_LIGHTLDA_PS_NUM("LIGHTLDA_NUM_PS"),

    THS_INPUT_FILE_LIST("INPUT_FILE_LIST"),

    THS_STAGING_LOCATION("THS_STAGING_LOCATION"),

    THS_CACHE_FILE_LOCATION("THS_CACHE_FILE_LOCATION"),

    THS_CACHE_ARCHIVE_LOCATION("THS_CACHE_ARCHIVE_LOCATION"),

    THS_FILES_LOCATION("THS_FILES_LOCATION"),

    APP_JAR_LOCATION("APP_JAR_LOCATION"),

    THS_JOB_CONF_LOCATION("THS_JOB_CONF_LOCATION"),

    THS_EXEC_CMD("THS_EXEC_CMD"),

    USER_PATH("USER_PATH"),

    USER_LD_LIBRARY_PATH("USER_LD_LIBRARY_PATH"),

    THS_OUTPUTS("THS_OUTPUTS"),

    THS_INPUTS("THS_INPUTS"),

    APPMASTER_HOST("APPMASTER_HOST"),

    APPMASTER_PORT("APPMASTER_PORT"),

    APP_ID("APP_ID"),

    APP_ATTEMPTID("APP_ATTEMPTID");

    private final String variable;

    Environment(String variable) {
      this.variable = variable;
    }

    public String key() {
      return variable;
    }

    public String toString() {
      return variable;
    }

    public String $() {
      if (Shell.WINDOWS) {
        return "%" + variable + "%";
      } else {
        return "$" + variable;
      }
    }

    @InterfaceAudience.Public
    @InterfaceStability.Unstable
    public String $$() {
      return ApplicationConstants.PARAMETER_EXPANSION_LEFT +
          variable +
          ApplicationConstants.PARAMETER_EXPANSION_RIGHT;
    }
  }
}
