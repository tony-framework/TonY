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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.MLFrameworkRuntime;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.horovod.HorovodClusterSpec;
import com.linkedin.tony.horovod.HorovodDriver;
import com.linkedin.tony.horovod.SlotInfo;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.util.Utils;

import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TEST_HOROVOD_FAIL;
import static com.linkedin.tony.TonyConfigurationKeys.TEST_HOROVOD_FAIL_ENABLE_KEY;

public class HorovodRuntime implements MLFrameworkRuntime {
    private static final Log LOG = LogFactory.getLog(HorovodRuntime.class);

    private HorovodDriver horovodDriver;

    @Override
    public String constructClusterSpec(TonySession session, String taskId) throws IOException {
        LOG.info("Starting Horovod Driver on AM...");

        TonySession.TonyTask tonyTask = session.getTask(taskId);
        String taskHost = tonyTask.getHost();

        List<Integer> sameHostIndexCollection = new ArrayList<>();
        String workerList = buildWorkerList(session, taskHost, sameHostIndexCollection);

        LOG.info("Horovod Worker host list: " + workerList);

        if (horovodDriver == null) {
            setInTestMode(session);
            HorovodDriver driver = null;
            try {
                driver = HorovodDriver.create(workerList);
                horovodDriver = driver;
                LOG.info("Horovod rendezvous server started, port: " + horovodDriver.getPort());
            } catch (Exception e) {
                LOG.error("Errors on starting horovod driver when all workers are ready.", e);
                session.setFinalStatus(FinalApplicationStatus.FAILED, "Failed to starting horovod driver.");
                session.setTrainingFinished();
                return null;
            } finally {
                if (driver != null) {
                    driver.close();
                }
            }
        }

        Collections.sort(sameHostIndexCollection);
        LOG.info("Same host name task index collection: " + sameHostIndexCollection);

        HorovodClusterSpec spec = new HorovodClusterSpec(
                horovodDriver.getSlotInfoList(),
                horovodDriver.getPort(),
                Utils.getCurrentHostName(),
                sameHostIndexCollection
        );
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(spec);
    }

    private void setInTestMode(TonySession session) {
        boolean enableFail = session.getTonyConf().getBoolean(TEST_HOROVOD_FAIL_ENABLE_KEY, DEFAULT_TEST_HOROVOD_FAIL);
        if (enableFail) {
            HorovodDriver.setInTest();
            HorovodDriver.setTaskFailInTestMode();
        }
    }

    @VisibleForTesting
    public String buildWorkerList(TonySession session, String taskHost, List<Integer> sameHostIndexCollection) {
        Map<String, Integer> hostNumProcMap = new HashMap<>();
        session.getTonyTasks().values().stream()
                .flatMap(tasks -> Arrays.stream(tasks))
                .filter(task -> task != null)
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
    public void destory() {
        if (horovodDriver != null) {
            horovodDriver.close();
            horovodDriver = null;
        }
    }

    @Override
    public void buildTaskEnv(TaskExecutor executor) throws Exception {
        LOG.info("Setting up Horovod job...");
        // cluster spec like: h1:1,h2:2,h3:1
        HorovodClusterSpec horovodClusterSpec =
                Utils.parseClusterSpecForHorovod(executor.getClusterSpec());
        setHorovodRunEnv(executor, horovodClusterSpec, executor.getTaskIndex(),
                Utils.getCurrentHostName());
    }

    private void setHorovodRunEnv(TaskExecutor executor, HorovodClusterSpec horovodClusterSpec,
            int taskIndex, String currentHostName) {
        int rendezvPort = horovodClusterSpec.getPort();
        String rendezvHost = horovodClusterSpec.getAmHost();
        LOG.info("Horovod rendezvous server host: " + rendezvHost + ", port: " + rendezvPort);

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

        LOG.info("TaskIndex: " + taskIndex + ", host: " + currentHostName + ", horovod local rank: "
                + assignSlotInfo.getLocalRank());

        LOG.info("Setting Horovod runtime env...");
        executor.getShellEnv().put("HOROVOD_CROSS_RANK", String.valueOf(assignSlotInfo.getCrossRank()));
        executor.getShellEnv().put("HOROVOD_CROSS_SIZE", String.valueOf(assignSlotInfo.getCrossSize()));
        executor.getShellEnv().put("HOROVOD_LOCAL_RANK", String.valueOf(assignSlotInfo.getLocalRank()));
        executor.getShellEnv().put("HOROVOD_LOCAL_SIZE", String.valueOf(assignSlotInfo.getLocalSize()));
        executor.getShellEnv().put("HOROVOD_SIZE", String.valueOf(assignSlotInfo.getSize()));
        executor.getShellEnv().put("HOROVOD_RANK", String.valueOf(assignSlotInfo.getRank()));

        executor.getShellEnv().put("HOROVOD_HOSTNAME", assignSlotInfo.getHostname());
    }
}