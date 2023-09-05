/*
 * Copyright 2021 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.tony.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.linkedin.tony.Constants;
import com.linkedin.tony.Framework;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.horovod.DriverCallbackInfo;
import com.linkedin.tony.horovod.HorovodClusterSpec;
import com.linkedin.tony.horovod.HorovodDriver;
import com.linkedin.tony.horovod.SlotInfo;
import com.linkedin.tony.TonySession;
import com.linkedin.tony.util.Utils;

import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_HOROVOD_DEBUG_MODE_ENABLE;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_IN_TEST_HOROVOD_MODE;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TEST_HOROVOD_FAIL;
import static com.linkedin.tony.TonyConfigurationKeys.DistributedMode.GANG;
import static com.linkedin.tony.TonyConfigurationKeys.HOROVOD_DRIVER_DEBUG_MODE_ENABLE;
import static com.linkedin.tony.TonyConfigurationKeys.IN_TEST_HOROVOD_MODE;
import static com.linkedin.tony.TonyConfigurationKeys.TEST_HOROVOD_FAIL_ENABLE_KEY;

public class HorovodRuntime extends MLGenericRuntime {
    private static final String DRIVER = "driver";
    private static final String WORKER = "worker";
    private static final List<String> ILLEGAL_CONFIG_REGEXS = Arrays.asList(
            "tony.driver\\.([a-z]+)"
    );
    private static final String DEBUG_DRIVER_CONF_KEY = "tony.driver.command";

    @Override
    public Framework.ApplicationMasterAdapter getAMAdapter() {
        return new HorovodAMAdapter();
    }

    @Override
    public Framework.TaskExecutorAdapter getTaskAdapter(TaskExecutor taskExecutor) {
        return new HorovodTaskAdapter(taskExecutor);
    }

    @Override
    public String getFrameworkType() {
        return TonyConfigurationKeys.FrameworkType.HOROVOD.name();
    }

    class HorovodAMAdapter extends AM {
        private volatile boolean isDriverReady = false;

        private List<SlotInfo> workerSlotMetaInfo;
        private String rendezvServerPort;
        private String rendezvServerHost;

        private boolean isDriverDebugMode = false;

        @Override
        public String constructClusterSpec(String taskId) throws IOException {
            assert session != null;

            TonySession.TonyTask tonyTask = session.getTask(taskId);
            String taskHost = tonyTask.getHost();

            List<Integer> sameHostIndexCollection = new ArrayList<>();
            String workerList = buildWorkerList(session, taskHost, sameHostIndexCollection);

            log.info("Horovod Worker host list: " + workerList);
            Collections.sort(sameHostIndexCollection);
            log.info("Same host name task index collection: " + sameHostIndexCollection);

            if (isDriverRole(taskId)) {
                log.info("starting Horovod Driver, worker list: " + workerList);
                return workerList;
            }

            if (!isDriverReady) {
                log.error("Horovod driver is not ready, it shouldn't return cluster spec to worker.");
                return null;
            }

            log.info("Starting Horovod worker, task id: " + taskId);
            // when task role is worker, it will return horovod cluster spec.
            HorovodClusterSpec spec = new HorovodClusterSpec(
                    workerSlotMetaInfo,
                    rendezvServerPort,
                    rendezvServerHost,
                    sameHostIndexCollection
            );
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(spec);
        }

        private boolean isDriverRole(String taskId) {
            assert session != null;

            if (DRIVER.equals(session.getTask(taskId).getJobName())) {
                return true;
            }

            return false;
        }

        @VisibleForTesting
        public String buildWorkerList(TonySession session, String taskHost, List<Integer> sameHostIndexCollection) {
            assert session != null;

            Map<String, Integer> hostNumProcMap = new HashMap<>();
            session.getTonyTasks().values().stream()
                    .flatMap(tasks -> Arrays.stream(tasks))
                    .filter(task -> task != null && !DRIVER.equals(task.getJobName()))
                    .forEach(task -> {
                        int numProc = hostNumProcMap.getOrDefault(task.getHost(), 0);
                        hostNumProcMap.put(task.getHost(), ++numProc);

                        if (task.getHost().equals(taskHost)) {
                            sameHostIndexCollection.add(Integer.parseInt(task.getTaskIndex()));
                        }
                    });

            String workerList = StringUtils.join(
                    hostNumProcMap.entrySet()
                            .stream()
                            .map(entry -> entry.getKey() + ":" + entry.getValue())
                            .collect(Collectors.toList()),
                    ","
            );
            return workerList;
        }


        @Override
        public boolean receiveTaskCallbackInfo(String taskId, String callbackInfo) {
            assert session != null;
            log.info("Receiving horovod driver call back info...");

            if (!isDriverRole(taskId)) {
                log.error("Accept call back info from not driver task executor.");
                return false;
            }

            DriverCallbackInfo driverCallbackInfo = new Gson().fromJson(callbackInfo, DriverCallbackInfo.class);
            this.workerSlotMetaInfo = driverCallbackInfo.getSlotInfos();
            this.rendezvServerPort = driverCallbackInfo.getPort();
            this.rendezvServerHost = driverCallbackInfo.getHost();

            this.isDriverReady = true;

            return true;
        }

        @Override
        public boolean canStartTask(TonyConfigurationKeys.DistributedMode distributedMode, String taskId) {
            assert session != null;

            if (GANG != distributedMode) {
                setAppFailed("Horovod don't support " + distributedMode + " distributed mode.");
                return false;
            }

            int numExpectedTasks = session.getNumExpectedTasks();

            if (session.getNumRegisteredTasks() != numExpectedTasks) {
                printTasksPeriodically();
                return false;
            }

            if (isDriverRole(taskId)) {
                return true;
            }

            // check driver is ready?
            if (!isDriverReady) {
                log.info("Horovod driver is not ready.");
                return false;
            }

            return true;
        }

        @Override
        public boolean validateAndUpdateConfig(Configuration tonyConf) {
            this.isDriverDebugMode = checkInDebugMode(tonyConf);

            if (this.isDriverDebugMode) {
                if (StringUtils.isEmpty(tonyConf.get(DEBUG_DRIVER_CONF_KEY))) {
                    log.error("Should set tony.driver.command conf when in horovod driver debug mode.");
                    return false;
                }
                return true;
            }

            // When not in debug mode, it will validate and update tonyConf.
            super.setIllegalConfKeyRegexs(ILLEGAL_CONFIG_REGEXS);
            if (!super.validateAndUpdateConfig(tonyConf)) {
                return false;
            }

            // inject driver conf and make it untracked.
            tonyConf.set("tony.driver.instances", "1");
            tonyConf.set("tony.driver.vcores", "1");
            tonyConf.set("tony.application.untracked.jobtypes", "driver");
            return true;
        }

        private void setAppFailed(String errorMsg) {
            session.setFinalStatus(FinalApplicationStatus.FAILED, errorMsg);
            session.setTrainingFinished();
        }
    }

    class HorovodTaskAdapter extends Task {
        public HorovodTaskAdapter(TaskExecutor executor) {
            super(executor);
        }

        protected void buildTaskEnv(TaskExecutor executor) throws Exception {
            log.info("Setting TonY task executor basic env...");
            Map<String, String> executorShellEnv = executor.getShellEnv();
            executorShellEnv.put(Constants.JOB_NAME, String.valueOf(executor.getJobName()));
            executorShellEnv.put(Constants.TASK_INDEX, String.valueOf(executor.getTaskIndex()));
            executorShellEnv.put(Constants.TASK_NUM, String.valueOf(executor.getNumTasks()));
            executorShellEnv.put(Constants.DISTRIBUTED_MODE_NAME, executor.getDistributedMode().name());
            executorShellEnv.put(Constants.CLUSTER_SPEC, executor.getClusterSpec());

            if (DRIVER.equals(executor.getJobName())) {
                log.info("Task is Horovod driver, no need to set extra env.");
                return;
            }
            log.info("Setting up Horovod worker...");

            // cluster spec like: h1:1,h2:2,h3:1
            HorovodClusterSpec horovodClusterSpec =
                    Utils.parseClusterSpecForHorovod(executor.getClusterSpec());
            setHorovodRunEnv(executor, horovodClusterSpec, executor.getTaskIndex(),
                    Utils.getCurrentHostName());
        }

        @Override
        public int run() throws Exception {
            assert taskExecutor != null;

            setInTestMode(taskExecutor);
            buildTaskEnv(taskExecutor);
            if (DRIVER.equals(taskExecutor.getJobName())) {
                String driverDebugCommand = null;
                if (checkInDebugMode(taskExecutor.getTonyConf())) {
                    driverDebugCommand = taskExecutor.getTonyConf().get(DEBUG_DRIVER_CONF_KEY);
                }

                HorovodDriver driver = HorovodDriver.create(
                        taskExecutor.getClusterSpec(),
                        taskExecutor.getShellEnv(),
                        taskExecutor.getTonyConf(),
                        driverDebugCommand
                );
                String callBackInfo = driver.getCallbackInfo();
                log.info("Horovod driver call back to AM: \n" + callBackInfo);
                String taskId = taskExecutor.getJobName() + ":" + taskExecutor.getTaskIndex();
                taskExecutor.callbackInfoToAM(taskId, callBackInfo);

                log.info("Horovod driver has started. It will end when all workers finished.");
                int exitCode = driver.waitFor();
                return exitCode;
            }

            return this.executorPythonShell(taskExecutor);
        }

        private void setInTestMode(TaskExecutor executor) {
            Configuration tonyConf = executor.getTonyConf();
            boolean isInTestMode = tonyConf.getBoolean(IN_TEST_HOROVOD_MODE, DEFAULT_IN_TEST_HOROVOD_MODE);
            if (isInTestMode) {
                HorovodDriver.setInTest();
            }

            boolean setFailedInTest = tonyConf.getBoolean(TEST_HOROVOD_FAIL_ENABLE_KEY, DEFAULT_TEST_HOROVOD_FAIL);
            if (setFailedInTest) {
                HorovodDriver.setInTest();
                HorovodDriver.setTaskFailInTestMode();
            }
        }

        private void setHorovodRunEnv(TaskExecutor executor, HorovodClusterSpec horovodClusterSpec,
                int taskIndex, String currentHostName) {
            String rendezvPort = horovodClusterSpec.getPort();
            String rendezvHost = horovodClusterSpec.getAmHost();
            log.info("Horovod rendezvous server host: " + rendezvHost + ", port: " + rendezvPort);

            executor.getShellEnv().put("HOROVOD_CONTROLLER", "gloo");
            executor.getShellEnv().put("HOROVOD_CPU_OPERATIONS", "gloo");
            executor.getShellEnv().put("HOROVOD_GLOO_TIMEOUT_SECONDS", "2000");
            executor.getShellEnv().put("HOROVOD_GLOO_RENDEZVOUS_PORT", String.valueOf(rendezvPort));
            executor.getShellEnv().put("HOROVOD_GLOO_RENDEZVOUS_ADDR", rendezvHost);

            List<SlotInfo> localRankSortList = new ArrayList<>();
            for (SlotInfo slotInfo : horovodClusterSpec.getSlotInfos()) {
                String hostName = slotInfo.getHostname();
                if (!hostName.equals(currentHostName)) {
                    continue;
                }

                localRankSortList.add(slotInfo);
                Collections.sort(localRankSortList, Comparator.comparingInt(SlotInfo::getLocalRank));
            }

            int seqIndex = horovodClusterSpec.getSameHostTaskIndexList().indexOf(taskIndex);
            SlotInfo assignSlotInfo = localRankSortList.get(seqIndex);

            log.info("TaskIndex: " + taskIndex + ", host: " + currentHostName + ", horovod local rank: "
                    + assignSlotInfo.getLocalRank());

            log.info("Setting Horovod runtime env...");
            executor.getShellEnv().put("HOROVOD_CROSS_RANK", String.valueOf(assignSlotInfo.getCrossRank()));
            executor.getShellEnv().put("HOROVOD_CROSS_SIZE", String.valueOf(assignSlotInfo.getCrossSize()));
            executor.getShellEnv().put("HOROVOD_LOCAL_RANK", String.valueOf(assignSlotInfo.getLocalRank()));
            executor.getShellEnv().put("HOROVOD_LOCAL_SIZE", String.valueOf(assignSlotInfo.getLocalSize()));
            executor.getShellEnv().put("HOROVOD_SIZE", String.valueOf(assignSlotInfo.getSize()));
            executor.getShellEnv().put("HOROVOD_RANK", String.valueOf(assignSlotInfo.getRank()));

            executor.getShellEnv().put("HOROVOD_HOSTNAME", assignSlotInfo.getHostname());
        }
    }

    private boolean checkInDebugMode(Configuration tonyConf) {
        return tonyConf.getBoolean(HOROVOD_DRIVER_DEBUG_MODE_ENABLE, DEFAULT_HOROVOD_DEBUG_MODE_ENABLE);
    }
}
