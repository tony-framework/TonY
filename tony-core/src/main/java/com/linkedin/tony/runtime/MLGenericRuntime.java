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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.AbstractFrameworkRuntime;
import com.linkedin.tony.Framework;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.TonySession;

import static com.linkedin.tony.Constants.SIDECAR_TB_ROLE_NAME;

public abstract class MLGenericRuntime extends AbstractFrameworkRuntime {
    private static final long REGISTRATION_STATUS_INTERVAL_MS = 15 * 1000;

    public Framework.ApplicationMasterAdapter getAMAdapter() {
        return new AM();
    }

    abstract public Framework.TaskExecutorAdapter getTaskAdapter(TaskExecutor taskExecutor);

    private boolean enableSidecarTB(Configuration tonyConf) {
        return StringUtils.isNotEmpty(tonyConf.get(TonyConfigurationKeys.TENSORBOARD_LOG_DIR));
    }

    class AM implements Framework.ApplicationMasterAdapter {
        protected TonySession session;
        private List<String> illegalConfKeyRegexs;
        private long lastRegisterWorkerTime = System.currentTimeMillis();
        private long runtimeInitialTime = System.currentTimeMillis();

        @Override
        public String constructClusterSpec(String taskId) throws IOException {
            assert session != null;
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(session.getClusterSpec());
        }

        @Override
        public void destroy() {
            // ignore
        }

        @Override
        public void setTonySession(TonySession session) {
            this.session = session;
        }

        @Override
        public boolean receiveTaskCallbackInfo(String taskId, String callbackInfo) {
            return true;
        }

        @Override
        public boolean canStartTask(TonyConfigurationKeys.DistributedMode distributedMode, String taskId) {
            assert session != null;
            switch (distributedMode) {
                case GANG:
                    int numExpectedTasks = session.getNumExpectedTasks();
                    if (session.getNumRegisteredTasks() == numExpectedTasks) {
                        log.info("All " + numExpectedTasks + " tasks registered.");
                        return true;
                    }
                    printTasksPeriodically();
                    return false;
                case FCFS:
                    return true;
                default:
                    log.error("Errors on registering to TonY AM, because of unknown distributed mode: "
                            + distributedMode);
                    return false;
            }
        }

        @Override
        public boolean validateAndUpdateConfig(Configuration tonyConf) {
            if (!validate(tonyConf)) {
                return false;
            }

            return true;
        }

        @Override
        public boolean isHealthy(Configuration tonyConf) {
            /**
             * Checking the containers allocation timeout.
             * When using GANG distributed mode, the training cluster will not be constructed until all containers are allocated from RM.
             * In this case, it will possibly cause deadlock when Yarn resources are not enough, refer to ISSUE:
             * https://github.com/linkedin/TonY/issues/573.
             * So it's necessary to release reserved container resources on AM when containers allocation timeout reached in GANG mode.
             */
            if (containerAllocationTimeout(tonyConf)) {
                session.setFinalStatus(FinalApplicationStatus.FAILED, "Container allocation timeout.");
                return false;
            }
            return true;
        }

        private boolean containerAllocationTimeout(Configuration tonyConf) {
            String distributedModeVal = tonyConf.get(TonyConfigurationKeys.APPLICATION_DISTRIBUTED_MODE,
                    TonyConfigurationKeys.DEFAULT_APPLICATION_DISTRIBUTED_MODE);
            TonyConfigurationKeys.DistributedMode distributedMode =
                    TonyConfigurationKeys.DistributedMode.valueOf(distributedModeVal.toUpperCase());
            if (distributedMode != TonyConfigurationKeys.DistributedMode.GANG) {
                return false;
            }

            // When not setting container allocation timeout, it will always return false.
            int containerAllocationTimeout = tonyConf.getInt(TonyConfigurationKeys.CONTAINER_ALLOCATION_TIMEOUT,
                    TonyConfigurationKeys.DEFAULT_CONTAINER_ALLOCATION_TIMEOUT);
            if (containerAllocationTimeout <= 0) {
                return false;
            }

            if (session.getTotalTasks() - session.getNumRegisteredTasks() > 0
                    && System.currentTimeMillis() - runtimeInitialTime > containerAllocationTimeout) {
                log.error("Container Allocation timeout, total required tasks number: " + session.getTotalTasks()
                        + ", allocated tasks number: " + session.getRegisteredTasks());
                return true;
            }
            return false;
        }

        @VisibleForTesting
        private boolean validate(Configuration tonyConf) {
            if (illegalConfKeyRegexs == null) {
                return true;
            }
            List<String> illegalKeys = illegalConfKeyRegexs.stream()
                    .map(regex -> tonyConf.getValByRegex(regex).keySet())
                    .flatMap(x -> x.stream()).collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(illegalKeys)) {
                log.error("Not allowed to configure illegal conf in Runtime. "
                        + "Illegal keys: " + illegalKeys);
                return false;
            }
            return true;
        }

        public void setIllegalConfKeyRegexs(List<String> illegalConfKeyRegexs) {
            this.illegalConfKeyRegexs = illegalConfKeyRegexs;
        }

        protected void printTasksPeriodically() {
            // Periodically print a list of all tasks we are still awaiting registration from.
            if (System.currentTimeMillis() - lastRegisterWorkerTime > REGISTRATION_STATUS_INTERVAL_MS) {
                Set<TonySession.TonyTask> unregisteredTasks = getUnregisteredTasks();
                log.info(String.format("Received registrations from %d tasks, awaiting registration from %d tasks.",
                        session.getNumRegisteredTasks(), session.getNumExpectedTasks() - session.getNumRegisteredTasks()));
                unregisteredTasks.forEach(t -> {
                    log.info(String.format("Awaiting registration from task %s %s in %s on host %s",
                            t.getJobName(), t.getTaskIndex(),
                            (t.getContainer() != null ? t.getContainer().getId().toString() : "none"),
                            (t.getContainer() != null ? t.getContainer().getNodeId().getHost() : "none")));
                });
                lastRegisterWorkerTime = System.currentTimeMillis();
            }
        }

        private Set<TonySession.TonyTask> getUnregisteredTasks() {
            assert session != null;
            return session.getTonyTasks().values().stream().flatMap(Arrays::stream)
                    .filter(task -> task != null && task.getHost() == null)
                    .collect(Collectors.toSet());
        }
    }

    abstract class Task implements Framework.TaskExecutorAdapter {
        protected TaskExecutor taskExecutor;

        public Task(TaskExecutor executor) {
            this.taskExecutor = executor;
        }

        @Override
        public boolean needReserveTBPort() {
            assert taskExecutor != null;

            boolean enableSidecarTB = enableSidecarTB(taskExecutor.getTonyConf());

            // When disable sidecar tensorboard, it will only be reserved on chief task executor.
            if (!enableSidecarTB && taskExecutor.isChief()) {
                return true;
            }

            // When enable sidecar tensorboard, it will only be reserved on sidecar executor.
            if (enableSidecarTB && isSidecarTBExecutor(taskExecutor)) {
                return true;
            }

            return false;
        }

        @Override
        public int run() throws Exception {
            assert taskExecutor != null;

            buildTaskEnv(taskExecutor);
            return executorPythonShell(taskExecutor);
        }

        private boolean isSidecarTBExecutor(TaskExecutor taskExecutor) {
            return SIDECAR_TB_ROLE_NAME.equals(taskExecutor.getJobName()) ? true : false;
        }

        protected abstract void buildTaskEnv(TaskExecutor executor) throws Exception;
    }
}
