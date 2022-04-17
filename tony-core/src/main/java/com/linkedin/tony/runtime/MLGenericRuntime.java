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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.AbstractFrameworkRuntime;
import com.linkedin.tony.Framework;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.TonySession;
import com.linkedin.tony.util.Utils;

import static com.linkedin.tony.Constants.SIDECAR_TB_ROLE_NAME;
import static com.linkedin.tony.TonyConfigurationKeys.CONTAINER_ALLOCATION_TIMEOUT;
import static com.linkedin.tony.TonyConfigurationKeys.getGroupDependentIgnoredKey;

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

        // Group dependencies policy.
        Map<String, List<String>> grpWithMembersIndex;
        Map<String, List<String>> taskInGrpsIndex;
        // todo: Need to support single group dependent multiple other groups
        Map<String, Pair<String, Long>> taskWithDependentGrpsIndex;

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
            String diagnostics = containerAllocationTimeout(tonyConf);
            if (diagnostics != null) {
                session.setFinalStatus(FinalApplicationStatus.FAILED, diagnostics);
                return false;
            }

            /**
             * Checking the task roles completion timeout when its' pre-dependency tasks finished
             *
             * For example, tensorflow estimator training job will include some roles of ps/worker/evaluator/chief.
             * Actually, due to the bug of tensorflow or misusing the estimator api, sometimes evaluator will hang.
             * So if we use the configuration as follows, when evaluator is still running after timeout and
             * chief/workers are finished, the mechanism of dependency group timeout will make job failed.
             *
             * Dependency group timeout configuration as follows:
             * ```
             * tony.application.group.A = worker,chief
             * tony.application.dependency.evaluator.timeout.after.A = 3600
             * ```
             *
             * And in some of the cases, we don't want to fail the whole job even though a dependency times out.
             * For example, if chief succeeded and there is a worker hanging for 1 hour,
             * users can configure the job to still pass. So it introduces the new config of
             * `tony.application.dependency.[X].timeout.after.[GROUP].ignored = true`, and more details could be
             * found in https://github.com/tony-framework/TonY/issues/641.
             *
             */
            String errorMsg = groupDependencyTimeout(tonyConf);
            if (errorMsg != null) {
                session.setFinalStatus(FinalApplicationStatus.FAILED, errorMsg);
                return false;
            }
            return true;
        }

        @VisibleForTesting
        protected String groupDependencyTimeout(Configuration tonyConf) {
            /**
             * precheck:
             * Is group dependency checking timeout enabled?
             * If not, directly return null.
             */
            if (taskWithDependentGrpsIndex == null) {
                taskWithDependentGrpsIndex = Utils.getJobTypeDependentGrps(tonyConf);
            }

            // groupDependencies is map, key: waiting role, value: pre-dependent groups and waiting timeout
            if (taskWithDependentGrpsIndex == null || taskWithDependentGrpsIndex.isEmpty()) {
                return null;
            }

            // groupMembers is map, key: groupName, value: its members in this group
            if (grpWithMembersIndex == null) {
                grpWithMembersIndex = Utils.getAllGroupJobTypes(tonyConf);
            }

            if (grpWithMembersIndex == null || grpWithMembersIndex.isEmpty()) {
                return null;
            }

            if (!session.allTasksScheduled()) {
                log.info("Group dependency timeout check will be ignored until all tasks scheduled.");
                return null;
            }

            // memberInGroups is map. key: jobtype name, value: in which groups
            if (taskInGrpsIndex == null) {
                taskInGrpsIndex = getMemberInGroups(grpWithMembersIndex);
            }

            Map<String, TonySession.TonyTask[]> allTasks = session.getTonyTasks();
            List<TonySession.TonyTask> runningTasks = session.getRunningTasks();

            // Get the running jobs' type, like the tf roles of ps/worker/chief/evaluator
            Set<String> runningJobTypes = runningTasks.stream()
                    .map(TonySession.TonyTask::getJobName)
                    .filter(jobname -> taskWithDependentGrpsIndex.containsKey(jobname))
                    .collect(Collectors.toSet());

            for (String runningTaskType : runningJobTypes) {
                Pair<String, Long> dependentGroupPair = taskWithDependentGrpsIndex.get(runningTaskType);
                String dependentGroupName = dependentGroupPair.getKey();
                long timeout = dependentGroupPair.getValue() * 1000;

                if (!grpWithMembersIndex.containsKey(dependentGroupName)) {
                    continue;
                }

                boolean allDependentTaskFinished = true;
                long latestEndTimeInAllDependentTasks = 0L;
                for (String dependentsGroupJobtype : grpWithMembersIndex.get(dependentGroupName)) {

                    if (Utils.existRunningTasksWithJobtype(runningTasks, dependentsGroupJobtype)) {
                        allDependentTaskFinished = false;
                        break;
                    }

                    // Find out the latest finished task in this task type, if the specified timeout exceed,
                    // make the job fail.
                    latestEndTimeInAllDependentTasks = Math.max(
                            Arrays.stream(allTasks.get(dependentsGroupJobtype))
                                    .mapToLong(x -> x.getEndTime())
                                    .max().getAsLong(),
                            latestEndTimeInAllDependentTasks
                    );
                }

                if (!allDependentTaskFinished) {
                    continue;
                }

                if (System.currentTimeMillis() - latestEndTimeInAllDependentTasks > timeout) {

                    String ignoredTaskTypeKey = getGroupDependentIgnoredKey(runningTaskType, dependentGroupName);
                    boolean ignoreTimeout = tonyConf.getBoolean(ignoredTaskTypeKey, false);
                    if (!ignoreTimeout) {
                        return String.format("Task type: %s runs exceeded timeout because it's "
                                        + "dependent group: %s (task set: [%s]) has been finished.",
                                runningTaskType, dependentGroupName,
                                StringUtils.join(grpWithMembersIndex.get(dependentGroupName), ","));
                    }

                    log.info(
                            String.format("Task type: %s is marked as untracked.", runningJobTypes)
                    );
                    session.makeTaskTypeUntracked(runningTaskType);
                }
            }

            return null;
        }

        private Map<String, List<String>> getMemberInGroups(Map<String, List<String>> groupMembers) {
            /**
             * key: job type name
             * value: the list of groups
             */
            Map<String, List<String>> memberInGroups = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : groupMembers.entrySet()) {
                String group = entry.getKey();
                List<String> members = entry.getValue();
                for (String member : members) {
                    memberInGroups.putIfAbsent(member, new ArrayList<>());
                    memberInGroups.get(member).add(group);
                }
            }
            return memberInGroups;
        }

        private String containerAllocationTimeout(Configuration tonyConf) {
            String distributedModeVal = tonyConf.get(TonyConfigurationKeys.APPLICATION_DISTRIBUTED_MODE,
                    TonyConfigurationKeys.DEFAULT_APPLICATION_DISTRIBUTED_MODE);
            TonyConfigurationKeys.DistributedMode distributedMode =
                    TonyConfigurationKeys.DistributedMode.valueOf(distributedModeVal.toUpperCase());
            if (distributedMode != TonyConfigurationKeys.DistributedMode.GANG) {
                return null;
            }

            // When not setting container allocation timeout, it will always return false.
            int containerAllocationTimeout = tonyConf.getInt(CONTAINER_ALLOCATION_TIMEOUT,
                    TonyConfigurationKeys.DEFAULT_CONTAINER_ALLOCATION_TIMEOUT);
            if (containerAllocationTimeout <= 0) {
                return null;
            }

            if (session.getTotalTasks() - session.getNumRegisteredTasks() > 0
                    && System.currentTimeMillis() - runtimeInitialTime > containerAllocationTimeout) {
                String diagnostics = "Task executors allocation timeout(" + CONTAINER_ALLOCATION_TIMEOUT
                        + "=" + containerAllocationTimeout + "). Total required number: "
                        + session.getTotalTasks() + ", allocated number: " + session.getRegisteredTasks();
                log.error(diagnostics);
                return diagnostics;
            }
            return null;
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
