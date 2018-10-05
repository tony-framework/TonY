/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.conf;

import com.linkedin.tony.common.TextMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

public class THSConfiguration extends YarnConfiguration {

  private static final String THS_DEFAULT_XML_FILE = "ths-default.xml";

  private static final String THS_SITE_XML_FILE = "ths-site.xml";

  static {
    YarnConfiguration.addDefaultResource(THS_DEFAULT_XML_FILE);
    YarnConfiguration.addDefaultResource(THS_SITE_XML_FILE);
  }

  public THSConfiguration() {
    super();
  }

  public THSConfiguration(Configuration conf) {
    super(conf);
  }

  /**
   * Configuration used in Client
   */
  public static final String DEFAULT_THS_APP_TYPE = "THS";

  public static final String THS_STAGING_DIR = "ths.staging.dir";

  public static final String DEFAULT_THS_STAGING_DIR = "/tmp/THS/staging";

  public static final String THS_LOG_PULL_INTERVAL = "ths.log.pull.interval";

  public static final int DEFAULT_THS_LOG_PULL_INTERVAL = 10000;

  public static final String THS_USER_CLASSPATH_FIRST = "ths.user.classpath.first";

  public static final boolean DEFAULT_THS_USER_CLASSPATH_FIRST = true;

  public static final String THS_HOST_LOCAL_ENABLE = "ths.host.local.enable";

  public static final boolean DEFAULT_THS_HOST_LOCAL_ENABLE = false;

  public static final String THS_REPORT_CONTAINER_STATUS = "ths.report.container.status";

  public static final boolean DEFAULT_THS_REPORT_CONTAINER_STATUS = true;

  public static final String THS_CONTAINER_MEM_USAGE_WARN_FRACTION = "ths.container.mem.usage.warn.fraction";

  public static final Double DEFAULT_THS_CONTAINER_MEM_USAGE_WARN_FRACTION = 0.70;

  public static final String THS_AM_MEMORY = "ths.am.memory";

  public static final int DEFAULT_THS_AM_MEMORY = 1024;

  public static final String THS_AM_CORES = "ths.am.cores";

  public static final int DEFAULT_THS_AM_CORES = 1;

  public static final String THS_WORKER_MEMORY = "ths.worker.memory";

  public static final int DEFAULT_THS_WORKER_MEMORY = 1024;

  public static final String THS_WORKER_VCORES = "ths.worker.cores";

  public static final int DEFAULT_THS_WORKER_VCORES = 1;

  public static final String THS_WORKER_NUM = "ths.worker.num";

  public static final int DEFAULT_THS_WORKER_NUM = 1;

  public static final String THS_PS_MEMORY = "ths.ps.memory";

  public static final int DEFAULT_THS_PS_MEMORY = 1024;

  public static final String THS_PS_VCORES = "ths.ps.cores";

  public static final int DEFAULT_THS_PS_VCORES = 1;

  public static final String THS_PS_NUM = "ths.ps.num";

  public static final int DEFAULT_THS_PS_NUM = 0;

  public static final String THS_WORKER_MEM_AUTO_SCALE = "ths.worker.mem.autoscale";

  public static final Double DEFAULT_THS_WORKER_MEM_AUTO_SCALE = 0.5;

  public static final String THS_PS_MEM_AUTO_SCALE = "ths.ps.mem.autoscale";

  public static final Double DEFAULT_THS_PS_MEM_AUTO_SCALE = 0.2;

  public static final String THS_APP_MAX_ATTEMPTS = "ths.app.max.attempts";

  public static final int DEFAULT_THS_APP_MAX_ATTEMPTS = 1;

  public static final String THS_MODE_SINGLE = "ths.mode.single";

  public static Boolean DEFAULT_THS_MODE_SINGLE = false;

  public static final String THS_APP_QUEUE = "ths.app.queue";

  public static final String DEFAULT_THS_APP_QUEUE = "DEFAULT";

  public static final String THS_APP_PRIORITY = "ths.app.priority";

  public static final int DEFAULT_THS_APP_PRIORITY = 3;

  public static final String THS_OUTPUT_LOCAL_DIR = "ths.output.local.dir";

  public static final String DEFAULT_THS_OUTPUT_LOCAL_DIR = "output";

  public static final String THS_INPUTF0RMAT_CLASS = "ths.inputformat.class";

  public static final Class<? extends InputFormat> DEFAULT_THS_INPUTF0RMAT_CLASS = org.apache.hadoop.mapred.TextInputFormat.class;

  public static final String THS_OUTPUTFORMAT_CLASS = "ths.outputformat.class";

  public static final Class<? extends OutputFormat> DEFAULT_THS_OUTPUTF0RMAT_CLASS = TextMultiOutputFormat.class;

  public static final String THS_INPUTFILE_RENAME = "ths.inputfile.rename";

  public static final Boolean DEFAULT_THS_INPUTFILE_RENAME = false;

  public static final String THS_INPUT_STRATEGY = "ths.input.strategy";

  public static final String DEFAULT_THS_INPUT_STRATEGY = "DOWNLOAD";

  public static final String THS_OUTPUT_STRATEGY = "ths.output.strategy";

  public static final String DEFAULT_THS_OUTPUT_STRATEGY = "UPLOAD";

  public static final String THS_STREAM_EPOCH = "ths.stream.epoch";

  public static final int DEFAULT_THS_STREAM_EPOCH = 1;

  public static final String THS_INPUT_STREAM_SHUFFLE = "ths.input.stream.shuffle";

  public static final Boolean DEFAULT_THS_INPUT_STREAM_SHUFFLE = false;

  public static final String THS_INPUTFORMAT_CACHESIZE_LIMIT= "ths.inputformat.cachesize.limit";

  public static final int DEFAULT_THS_INPUTFORMAT_CACHESIZE_LIMIT = 100 * 1024;

  public static final String THS_INPUTFORMAT_CACHE = "ths.inputformat.cache";

  public static final boolean DEFAULT_THS_INPUTFORMAT_CACHE = false;

  public static final String THS_INPUTFORMAT_CACHEFILE_NAME = "ths.inputformat.cachefile.name";

  public static final String DEFAULT_THS_INPUTFORMAT_CACHEFILE_NAME = "inputformatCache.gz";

  public static final String THS_INTERREAULST_DIR = "ths.interresult.dir";

  public static final String DEFAULT_THS_INTERRESULT_DIR = "/interResult_";

  public static final String[] DEFAULT_THS_APPLICATION_CLASSPATH = {
      "$HADOOP_CONF_DIR",
      "$HADOOP_COMMON_HOME/share/hadoop/common/*",
      "$HADOOP_COMMON_HOME/share/hadoop/common/lib/*",
      "$HADOOP_HDFS_HOME/share/hadoop/hdfs/*",
      "$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*",
      "$HADOOP_YARN_HOME/share/hadoop/yarn/*",
      "$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*",
      "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*",
      "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*"
  };

  public static final String THS_TF_BOARD_PATH = "ths.tf.board.path";
  public static final String DEFAULT_THS_TF_BOARD_PATH = "tensorboard";
  public static final String THS_TF_BOARD_WORKER_INDEX = "ths.tf.board.worker.index";
  public static final int DEFAULT_THS_TF_BOARD_WORKER_INDEX = 0;
  public static final String THS_TF_BOARD_RELOAD_INTERVAL = "ths.tf.board.reload.interval";
  public static final int DEFAULT_THS_TF_BOARD_RELOAD_INTERVAL = 1;
  public static final String THS_TF_BOARD_LOG_DIR = "ths.tf.board.log.dir";
  public static final String DEFAULT_THS_TF_BOARD_LOG_DIR = "eventLog";
  public static final String THS_TF_BOARD_ENABLE = "ths.tf.board.enable";
  public static final Boolean DEFAULT_THS_TF_BOARD_ENABLE = true;
  public static final String THS_TF_BOARD_HISTORY_DIR = "ths.tf.board.history.dir";
  public static final String DEFAULT_THS_TF_BOARD_HISTORY_DIR = "/tmp/THS/eventLog";
  public static final String THS_BOARD_PATH = "ths.board.path";
  public static final String DEFAULT_THS_BOARD_PATH = "visualDL";
  public static final String THS_BOARD_MODELPB = "ths.board.modelpb";
  public static final String DEFAULT_THS_BOARD_MODELPB = "";
  public static final String THS_BOARD_CACHE_TIMEOUT = "ths.board.cache.timeout";
  public static final int DEFAULT_THS_BOARD_CACHE_TIMEOUT = 20;
  /**
   * Configuration used in ApplicationMaster
   */
  public static final String THS_CONTAINER_EXTRA_JAVA_OPTS = "ths.container.extra.java.opts";

  public static final String DEFAULT_THS_CONTAINER_JAVA_OPTS_EXCEPT_MEMORY = "";

  public static final String THS_ALLOCATE_INTERVAL = "ths.allocate.interval";

  public static final int DEFAULT_THS_ALLOCATE_INTERVAL = 1000;

  public static final String THS_STATUS_UPDATE_INTERVAL = "ths.status.update.interval";

  public static final int DEFAULT_THS_STATUS_PULL_INTERVAL = 1000;

  public static final String THS_TASK_TIMEOUT = "ths.task.timeout";

  public static final int DEFAULT_THS_TASK_TIMEOUT = 5 * 60 * 1000;

  public static final String THS_LOCALRESOURCE_TIMEOUT = "ths.localresource.timeout";

  public static final int DEFAULT_THS_LOCALRESOURCE_TIMEOUT = 5 * 60 * 1000;

  public static final String THS_TASK_TIMEOUT_CHECK_INTERVAL_MS = "ths.task.timeout.check.interval";

  public static final int DEFAULT_THS_TASK_TIMEOUT_CHECK_INTERVAL_MS = 3 * 1000;

  public static final String THS_INTERRESULT_UPLOAD_TIMEOUT = "ths.interresult.upload.timeout";

  public static final int DEFAULT_THS_INTERRESULT_UPLOAD_TIMEOUT = 50 * 60 * 1000;

  public static final String THS_MESSAGES_LEN_MAX = "ths.messages.len.max";

  public static final int DEFAULT_THS_MESSAGES_LEN_MAX = 1000;

  public static final String THS_EXECUTE_NODE_LIMIT = "ths.execute.node.limit";

  public static final int DEFAULT_THS_EXECUTENODE_LIMIT = 200;

  public static final String THS_CLEANUP_ENABLE = "ths.cleanup.enable";

  public static final boolean DEFAULT_THS_CLEANUP_ENABLE = true;

  public static final String THS_CONTAINER_MAX_FAILURES_RATE = "ths.container.maxFailures.rate";

  public static final double DEFAULT_THS_CONTAINER_FAILURES_RATE = 0.5;

  public static final String THS_ENV_MAXLENGTH = "ths.env.maxlength";

  public static final Integer DEFAULT_THS_ENV_MAXLENGTH = 102400;

  /**
   * Configuration used in Container
   */
  public static final String THS_DOWNLOAD_FILE_RETRY = "ths.download.file.retry";

  public static final int DEFAULT_THS_DOWNLOAD_FILE_RETRY = 3;

  public static final String THS_DOWNLOAD_FILE_THREAD_NUMS = "ths.download.file.thread.nums";

  public static final int DEFAULT_THS_DOWNLOAD_FILE_THREAD_NUMS = 10;

  public static final String THS_CONTAINER_HEARTBEAT_INTERVAL = "ths.container.heartbeat.interval";

  public static final int DEFAULT_THS_CONTAINER_HEARTBEAT_INTERVAL = 10 * 1000;

  public static final String THS_CONTAINER_HEARTBEAT_RETRY = "ths.container.heartbeat.retry";

  public static final int DEFAULT_THS_CONTAINER_HEARTBEAT_RETRY = 3;

  public static final String THS_CONTAINER_UPDATE_APP_STATUS_INTERVAL = "ths.container.update.appstatus.interval";

  public static final int DEFAULT_THS_CONTAINER_UPDATE_APP_STATUS_INTERVAL = 3 * 1000;

  public static final String THS_CONTAINER_AUTO_CREATE_OUTPUT_DIR = "ths.container.auto.create.output.dir";

  public static final boolean DEFAULT_THS_CONTAINER_AUTO_CREATE_OUTPUT_DIR = true;

  /**
   * Configuration used in Log Dir
   */
  public static final String THS_HISTORY_LOG_DIR = "ths.history.log.dir";

  public static final String DEFAULT_THS_HISTORY_LOG_DIR = "/tmp/THS/history";

  public static final String THS_HISTORY_LOG_DELETE_MONITOR_TIME_INTERVAL = "ths.history.log.delete-monitor-time-interval";

  public static final int DEFAULT_THS_HISTORY_LOG_DELETE_MONITOR_TIME_INTERVAL = 24 * 60 * 60 * 1000;

  public static final String THS_HISTORY_LOG_MAX_AGE_MS = "ths.history.log.max-age-ms";

  public static final int DEFAULT_THS_HISTORY_LOG_MAX_AGE_MS = 24 * 60 * 60 * 1000;

  /**
   * Configuration used in Job History
   */
  public static final String THS_HISTORY_ADDRESS = "ths.history.address";

  public static final String THS_HISTORY_PORT = "ths.history.port";

  public static final int DEFAULT_THS_HISTORY_PORT = 10021;

  public static final String DEFAULT_THS_HISTORY_ADDRESS = "0.0.0.0:" + DEFAULT_THS_HISTORY_PORT;

  public static final String THS_HISTORY_WEBAPP_ADDRESS = "ths.history.webapp.address";

  public static final String THS_HISTORY_WEBAPP_PORT = "ths.history.webapp.port";

  public static final int DEFAULT_THS_HISTORY_WEBAPP_PORT = 19886;

  public static final String DEFAULT_THS_HISTORY_WEBAPP_ADDRESS = "0.0.0.0:" + DEFAULT_THS_HISTORY_WEBAPP_PORT;

  public static final String THS_HISTORY_WEBAPP_HTTPS_ADDRESS = "ths.history.webapp.https.address";

  public static final String THS_HISTORY_WEBAPP_HTTPS_PORT = "ths.history.webapp.https.port";

  public static final int DEFAULT_THS_HISTORY_WEBAPP_HTTPS_PORT = 19885;

  public static final String DEFAULT_THS_HISTORY_WEBAPP_HTTPS_ADDRESS = "0.0.0.0:" + DEFAULT_THS_HISTORY_WEBAPP_HTTPS_PORT;

  public static final String THS_HISTORY_BIND_HOST = "ths.history.bind-host";

  public static final String THS_HISTORY_CLIENT_THREAD_COUNT = "ths.history.client.thread-count";

  public static final int DEFAULT_THS_HISTORY_CLIENT_THREAD_COUNT = 10;

  public static final String THS_HS_RECOVERY_ENABLE = "ths.history.recovery.enable";

  public static final boolean DEFAULT_THS_HS_RECOVERY_ENABLE = false;

  public static final String THS_HISTORY_KEYTAB = "ths.history.keytab";

  public static final String THS_HISTORY_PRINCIPAL = "ths.history.principal";

  /**
   * To enable https in THS history server
   */
  public static final String THS_HS_HTTP_POLICY = "ths.history.http.policy";
  public static String DEFAULT_THS_HS_HTTP_POLICY =
      HttpConfig.Policy.HTTP_ONLY.name();

  /**
   * The kerberos principal to be used for spnego filter for history server
   */
  public static final String THS_WEBAPP_SPNEGO_USER_NAME_KEY = "ths.webapp.spnego-principal";

  /**
   * The kerberos keytab to be used for spnego filter for history server
   */
  public static final String THS_WEBAPP_SPNEGO_KEYTAB_FILE_KEY = "ths.webapp.spnego-keytab-file";

  //Delegation token related keys
  public static final String DELEGATION_KEY_UPDATE_INTERVAL_KEY =
      "ths.cluster.delegation.key.update-interval";
  public static final long DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT =
      24 * 60 * 60 * 1000; // 1 day
  public static final String DELEGATION_TOKEN_RENEW_INTERVAL_KEY =
      "ths.cluster.delegation.token.renew-interval";
  public static final long DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT =
      24 * 60 * 60 * 1000;  // 1 day
  public static final String DELEGATION_TOKEN_MAX_LIFETIME_KEY =
      "ths.cluster.delegation.token.max-lifetime";
  public static final long DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT =
      24 * 60 * 60 * 1000; // 7 days
}
