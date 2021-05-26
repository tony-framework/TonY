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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.FrameworkRuntime;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.util.Utils;

import static com.linkedin.tony.Constants.SIDECAR_TB_TEST_KEY;
import static com.linkedin.tony.Constants.SIDECAR_TENSORBOARD_ROLE_NAME;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_GPUS;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_INSTANCES;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_MEMORY;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_VCORE;
import static com.linkedin.tony.TonyConfigurationKeys.SIDECAR_JOBTYPES;
import static com.linkedin.tony.TonyConfigurationKeys.TB_GPUS;
import static com.linkedin.tony.TonyConfigurationKeys.TB_INSTANCES;
import static com.linkedin.tony.TonyConfigurationKeys.TB_MEMORY;
import static com.linkedin.tony.TonyConfigurationKeys.TB_VCORE;

public abstract class MLGenericRuntime implements FrameworkRuntime {
    private static final long REGISTRATION_STATUS_INTERVAL_MS = 15 * 1000;

    private static final String SIDECAR_TENSORBOARD_DIR_NAME = "sidecar_tensorboard";
    private static final String SIDECAR_TENSORBORD_SCRIPT = "sidecar_tensorboard.py";

    // when in AM, session should be set. In task executor, session will be null.
    protected TonySession session;
    protected Log log = LogFactory.getLog(this.getClass());
    private List<String> illegalConfKeyRegexs;
    private long lastRegisterWorkerTime = System.currentTimeMillis();

    protected TaskExecutor taskExecutor;

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

        return checkAndPrepareTBResource(tonyConf);
    }

    /**
     * Set the required resources when enable auto-starting tensorboard.
     */
    @VisibleForTesting
    protected boolean checkAndPrepareTBResource(Configuration tonyConf) {
        if (!enableSidecarTB(tonyConf)) {
            return true;
        }

        if (StringUtils.isNotEmpty(tonyConf.get(TB_INSTANCES))) {
            log.error("When enable side-car tensorboard, specifying the conf of " + TB_INSTANCES + " is not allowed.");
            return false;
        }

        tonyConf.set(TB_INSTANCES, String.valueOf(DEFAULT_TB_INSTANCES));
        tonyConf.set(TB_VCORE, tonyConf.get(TB_VCORE, String.valueOf(DEFAULT_TB_VCORE)));
        tonyConf.set(TB_MEMORY, tonyConf.get(TB_MEMORY, DEFAULT_TB_MEMORY));
        tonyConf.set(TB_GPUS, tonyConf.get(TB_GPUS, String.valueOf(DEFAULT_TB_GPUS)));

        List<String> sidecarTypes = new ArrayList<>(Arrays.asList(Utils.getSidecarJobTypes(tonyConf)));
        sidecarTypes.add(SIDECAR_TENSORBOARD_ROLE_NAME);
        tonyConf.set(SIDECAR_JOBTYPES, StringUtils.join(sidecarTypes, ","));

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
    public void initTaskExecutorResource(TaskExecutor executor) {
        this.taskExecutor = executor;
    }

    /**
     * The tensorboard port will only be reserved on chief or sidecar tensorboard executor.
     * Other executors will not reserve it.
     * However, the above reserved port action will not be at the same time.
     * If enable side-car tensorboard, port will only be reserved on it, otherwise will only be on chief executor.
     */
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

        if (isSidecarTBExecutor(taskExecutor)) {
            // inject starting tensorboard command
            setStartupTBResource();
        }

        buildTaskEnv(taskExecutor);
        return executorPythonShell(taskExecutor);
    }

    private void setStartupTBResource() {
        String tbLogDir = taskExecutor.getTonyConf().get(TonyConfigurationKeys.TENSORBOARD_LOG_DIR);
        Path tbScriptPath = createSidecarTBScript();
        String pythonExecPath = taskExecutor.getTonyConf().get(TonyConfigurationKeys.PYTHON_EXEC_PATH, "python");
        String startTBCommand = String.format("%s %s --logdir %s --port %s",
                pythonExecPath, tbScriptPath, tbLogDir, taskExecutor.getTbPort());
        log.info("Sidecar tensorboard startup command: " + startTBCommand);
        taskExecutor.setTaskCommand(startTBCommand);

        if (System.getenv(SIDECAR_TB_TEST_KEY) != null) {
            taskExecutor.getShellEnv().put(SIDECAR_TB_TEST_KEY, "true");
        }
    }

    private boolean isSidecarTBExecutor(TaskExecutor taskExecutor) {
        return SIDECAR_TENSORBOARD_ROLE_NAME.equals(taskExecutor.getJobName()) ? true : false;
    }

    private boolean enableSidecarTB(Configuration tonyConf) {
        return StringUtils.isNotEmpty(tonyConf.get(TonyConfigurationKeys.TENSORBOARD_LOG_DIR));
    }

    private Path createSidecarTBScript() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final String sidecarTBScript = SIDECAR_TENSORBORD_SCRIPT;
        try {
            Path tempDir = Files.createTempDirectory(SIDECAR_TENSORBOARD_DIR_NAME);
            tempDir.toFile().deleteOnExit();
            try (InputStream stream = loader.getResourceAsStream(sidecarTBScript)) {
                Files.copy(stream, Paths.get(tempDir.toAbsolutePath().toString(), sidecarTBScript));
            }
            return Paths.get(tempDir.toAbsolutePath().toString(), sidecarTBScript);
        } catch (Exception e) {
            log.info(e);
            return null;
        }
    }

    protected abstract void buildTaskEnv(TaskExecutor executor) throws Exception;
}
