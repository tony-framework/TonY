/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.jobhistory;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.conf.THSConfiguration;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.HistoryServerStateStoreService;
import org.apache.hadoop.mapreduce.v2.hs.HistoryServerStateStoreService.HistoryServerState;
import org.apache.hadoop.mapreduce.v2.hs.HistoryServerStateStoreServiceFactory;
import org.apache.hadoop.mapreduce.v2.hs.JHSDelegationTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

public class JobHistoryServer extends CompositeService {

  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(JobHistoryServer.class);
  protected HistoryContext historyContext;
  private HistoryClientService clientService;
  private JobHistory jobHistoryService;
  protected JHSDelegationTokenSecretManager jhsDTSecretManager;
  private AggregatedLogDeletionService aggLogDelService;
  private HistoryServerStateStoreService stateStore;
  private Thread deleteLogManager;

  private class HistoryServerSecretManagerService
      extends AbstractService {

    public HistoryServerSecretManagerService() {
      super(HistoryServerSecretManagerService.class.getName());
    }

    @Override
    protected void serviceStart() throws Exception {
      boolean recoveryEnabled = getConfig().getBoolean(
          THSConfiguration.THS_HS_RECOVERY_ENABLE,
          THSConfiguration.DEFAULT_THS_HS_RECOVERY_ENABLE);
      if (recoveryEnabled) {
        assert stateStore.isInState(STATE.STARTED);
        HistoryServerState state = stateStore.loadState();
        jhsDTSecretManager.recover(state);
      }

      try {
        jhsDTSecretManager.startThreads();
      } catch (IOException io) {
        LOG.error("Error while starting the Secret Manager threads", io);
        throw io;
      }

      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
      if (jhsDTSecretManager != null) {
        jhsDTSecretManager.stopThreads();
      }
      super.serviceStop();
    }
  }

  public JobHistoryServer() {
    super(JobHistoryServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Configuration config = new THSConfiguration(conf);
    // Not applicable in Java 8
    //  config.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    // This is required for WebApps to use https if enabled.
    THSWebAppUtil.initialize(getConfig());

    try {
      doSecureLogin(conf);
    } catch (IOException ie) {
      throw new YarnRuntimeException("History Server Failed to login", ie);
    }

    jobHistoryService = new JobHistory();
    historyContext = (HistoryContext) jobHistoryService;
    stateStore = createStateStore(conf);
    this.jhsDTSecretManager = createJHSSecretManager(conf, stateStore);
    clientService = createHistoryClientService();
    aggLogDelService = new AggregatedLogDeletionService();

    addService(stateStore);
    addService(new HistoryServerSecretManagerService());
    addService(clientService);
    addService(aggLogDelService);
    super.serviceInit(config);
  }

  @VisibleForTesting
  protected HistoryClientService createHistoryClientService() {
    return new HistoryClientService(historyContext,
        this.jhsDTSecretManager);
  }

  protected JHSDelegationTokenSecretManager createJHSSecretManager(
      Configuration conf, HistoryServerStateStoreService store) {
    long secretKeyInterval =
        conf.getLong(THSConfiguration.DELEGATION_KEY_UPDATE_INTERVAL_KEY,
            THSConfiguration.DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
    long tokenMaxLifetime =
        conf.getLong(THSConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            THSConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    long tokenRenewInterval =
        conf.getLong(THSConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            THSConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);

    return new JHSDelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval, 3600000, store);
  }

  protected HistoryServerStateStoreService createStateStore(
      Configuration conf) {
    return HistoryServerStateStoreServiceFactory.getStore(conf);
  }

  protected void doSecureLogin(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getBindAddress(conf);
    SecurityUtil.login(conf, THSConfiguration.THS_HISTORY_KEYTAB,
        THSConfiguration.THS_HISTORY_PRINCIPAL, socAddr.getHostName());
  }

  /**
   * Retrieve JHS bind address from configuration
   *
   * @param conf
   * @return InetSocketAddress
   */
  public static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(THSConfiguration.THS_HISTORY_ADDRESS,
        conf.get(THSConfiguration.THS_HISTORY_ADDRESS, THSConfiguration.DEFAULT_THS_HISTORY_ADDRESS),
        conf.getInt(THSConfiguration.THS_HISTORY_PORT, THSConfiguration.DEFAULT_THS_HISTORY_PORT));
  }

  private class deleteLogMonitor implements Runnable {

    @Override
    public void run() {
      FileSystem fs;
      Configuration conf = new THSConfiguration();
      Path historyLog = new Path(conf.get(THSConfiguration.THS_HISTORY_LOG_DIR,
          THSConfiguration.DEFAULT_THS_HISTORY_LOG_DIR));
      Path eventLog = new Path(conf.get(THSConfiguration.THS_TF_BOARD_HISTORY_DIR,
          THSConfiguration.DEFAULT_THS_TF_BOARD_HISTORY_DIR));
      int monitorInterval = conf.getInt(THSConfiguration.THS_HISTORY_LOG_DELETE_MONITOR_TIME_INTERVAL,
          THSConfiguration.DEFAULT_THS_HISTORY_LOG_DELETE_MONITOR_TIME_INTERVAL);
      int logMaxAge = conf.getInt(THSConfiguration.THS_HISTORY_LOG_MAX_AGE_MS,
          THSConfiguration.DEFAULT_THS_HISTORY_LOG_MAX_AGE_MS);
      final Clock clock = new SystemClock();
      while (!Thread.currentThread().isInterrupted()) {
        try {
          LOG.info("Start delete log thread.");
          Long currentClock = clock.getTime();
          fs = FileSystem.get(conf);
          FileStatus[] allHistoryLog = fs.listStatus(historyLog);
          LOG.info("historyLog:" + historyLog);
          for (FileStatus historyLogPer : allHistoryLog) {
            LOG.info(historyLogPer.getPath() + ":" + String.valueOf(currentClock - historyLogPer.getModificationTime()));
            if ((currentClock - historyLogPer.getModificationTime()) > logMaxAge) {
              fs.delete(historyLogPer.getPath(), true);
            }
          }
          FileStatus[] allEventLog = fs.listStatus(eventLog);
          LOG.info("eventLog:" + eventLog);
          for (FileStatus eventLogPer : allEventLog) {
            LOG.info(eventLogPer.getPath() + ":" + String.valueOf(currentClock - eventLogPer.getModificationTime()));
            if ((currentClock - eventLogPer.getModificationTime()) > logMaxAge) {
              fs.delete(eventLogPer.getPath());
            }
          }

          Thread.sleep(monitorInterval);
        } catch (Exception e) {
          LOG.info("HistoryLog delete thread interrupted. " + e);
          break;
        }
      }
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    DefaultMetricsSystem.initialize("JobHistoryServer");
    JvmMetrics.initSingleton("JobHistoryServer", null);
    super.serviceStart();

    deleteLogManager = new Thread(new deleteLogMonitor());
    deleteLogManager.setName("Log-delete-monitor");
    deleteLogManager.setDaemon(true);
    deleteLogManager.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    DefaultMetricsSystem.shutdown();
    super.serviceStop();
  }

  static JobHistoryServer launchJobHistoryServer(String[] args) {
    Thread.
        setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(JobHistoryServer.class, args, LOG);
    JobHistoryServer jobHistoryServer = null;
    try {
      jobHistoryServer = new JobHistoryServer();
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(jobHistoryServer),
          SHUTDOWN_HOOK_PRIORITY);
      YarnConfiguration conf = new YarnConfiguration(new JobConf());
      new GenericOptionsParser(conf, args);
      jobHistoryServer.init(conf);
      jobHistoryServer.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting JobHistoryServer", t);
      ExitUtil.terminate(-1, "Error starting JobHistoryServer");
    }
    return jobHistoryServer;
  }

  public static void main(String[] args) {
    launchJobHistoryServer(args);
  }
}
