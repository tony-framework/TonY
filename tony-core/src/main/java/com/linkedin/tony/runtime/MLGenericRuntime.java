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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.FrameworkRuntime;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.tensorflow.TonySession;

public class MLGenericRuntime implements FrameworkRuntime {
    private static final long REGISTRATION_STATUS_INTERVAL_MS = 15 * 1000;

    // when in AM, session should be set. In task executor, session will be null.
    protected TonySession session;
    protected Log log = LogFactory.getLog(this.getClass());
    private List<String> illegalConfKeyRegexs;
    private long lastRegisterWorkerTime = System.currentTimeMillis();

    // ===================For AM =======================

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

    // ===================For Task Executor=======================

    @Override
    public int run(TaskExecutor executor) throws Exception {
        buildTaskEnv(executor);
        return executorPythonShell(executor);
    }

    protected void buildTaskEnv(TaskExecutor executor) throws Exception {
        return;
    }
}
