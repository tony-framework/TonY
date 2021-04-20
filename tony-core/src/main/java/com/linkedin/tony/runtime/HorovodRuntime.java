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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.horovod.DriverCallbackInfo;
import com.linkedin.tony.horovod.HorovodClusterSpec;
import com.linkedin.tony.horovod.HorovodDriver;
import com.linkedin.tony.horovod.SlotInfo;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.util.Utils;

import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_IN_TEST_HOROVOD_MODE;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TEST_HOROVOD_FAIL;
import static com.linkedin.tony.TonyConfigurationKeys.DistributedMode.GANG;
import static com.linkedin.tony.TonyConfigurationKeys.IN_TEST_HOROVOD_MODE;
import static com.linkedin.tony.TonyConfigurationKeys.TEST_HOROVOD_FAIL_ENABLE_KEY;

public class HorovodRuntime extends MLGenericRuntime {
    private static final String DRIVER = "driver";
    private static final String WORKER = "worker";

    private volatile boolean isDriverReady = false;

    private List<SlotInfo> workerSlotMetaInfo;
    private String rendezvServerPort;
    private String rendezvServerHost;

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
            return workerList;
        }

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

    private void setInTestMode() {
        assert session != null;

        boolean isInTestMode = session.getTonyConf().getBoolean(IN_TEST_HOROVOD_MODE, DEFAULT_IN_TEST_HOROVOD_MODE);
        if (isInTestMode) {
            HorovodDriver.setInTest();
        }

        boolean setFailedInTest = session.getTonyConf().getBoolean(TEST_HOROVOD_FAIL_ENABLE_KEY, DEFAULT_TEST_HOROVOD_FAIL);
        if (setFailedInTest) {
            HorovodDriver.setInTest();
            HorovodDriver.setTaskFailInTestMode();
        }
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

        // check driver is ready?
        if (!isDriverReady) {
            log.info("Horovod driver is not ready.");
            return false;
        }

        return true;
    }

    @Override
    public boolean validateAndUpdateConfig(Configuration tonyConf) {
        // inject driver conf and make it untracked.
        tonyConf.set("tony.driver.instances", "1");
        tonyConf.set("tony.driver.vcores", "1");
        tonyConf.set("tony.application.untracked.jobtypes", "driver");
        return true;
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

    private void setAppFailed(String errorMsg) {
        session.setFinalStatus(FinalApplicationStatus.FAILED, errorMsg);
        session.setTrainingFinished();
    }

    // ===================For task executor=======================

    @Override
    public void buildTaskEnv(TaskExecutor executor) throws Exception {
        log.info("Setting up Horovod job...");
        if (DRIVER.equals(executor.getJobName())) {
            log.info("Task is Horovod driver, no need to set extra env.");
            return;
        }

        // cluster spec like: h1:1,h2:2,h3:1
        HorovodClusterSpec horovodClusterSpec =
                Utils.parseClusterSpecForHorovod(executor.getClusterSpec());
        setHorovodRunEnv(executor, horovodClusterSpec, executor.getTaskIndex(),
                Utils.getCurrentHostName());

        Map<String, String> executorShellEnv = executor.getShellEnv();
        executorShellEnv.put(Constants.JOB_NAME, String.valueOf(executor.getJobName()));
        executorShellEnv.put(Constants.TASK_INDEX, String.valueOf(executor.getTaskIndex()));
        executorShellEnv.put(Constants.TASK_NUM, String.valueOf(executor.getNumTasks()));
        executorShellEnv.put(Constants.DISTRUBUTED_MODE_NAME, executor.getDistributedMode().name());
    }

    @Override
    public int run(TaskExecutor executor) throws Exception {
        setInTestMode();

        buildTaskEnv(executor);
        if (DRIVER.equals(executor.getJobName())) {
            // TODO: 2021/4/13  if driver failed, it should fast fail. AM should fail. Unit test should cover this.

            if (session.getTonyConf().getBoolean(TEST_HOROVOD_FAIL_ENABLE_KEY, false)) {
                HorovodDriver.setInTest();
                HorovodDriver.setTaskFailInTestMode();
            }

            HorovodDriver driver = HorovodDriver.create(executor.getClusterSpec());
            String callBackInfo = driver.getCallbackInfo();
            log.info("Horovod driver call back to AM: \n" + callBackInfo);
            String taskId = executor.getJobName() + ":" + executor.getTaskIndex();
            executor.callbackInfoToAM(taskId, callBackInfo);
            int exitCode = driver.waitFor();
            return exitCode;
        }

        return this.executorPythonShell(executor);
    }
}