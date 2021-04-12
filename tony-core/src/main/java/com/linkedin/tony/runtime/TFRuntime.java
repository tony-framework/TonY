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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.tony.Constants;
import com.linkedin.tony.MLFrameworkRuntime;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.util.Utils;

public class TFRuntime implements MLFrameworkRuntime {
    private static final Log LOG = LogFactory.getLog(TFRuntime.class);

    @Override
    public String constructClusterSpec(TonySession session, String taskId) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(session.getClusterSpec());
    }

    @Override
    public void destory() {
        // ignore
    }

    @Override
    public void buildTaskEnv(TaskExecutor executor) throws Exception {
        LOG.info("Setting up TensorFlow job...");
        executor.getShellEnv().put(Constants.JOB_NAME, String.valueOf(executor.getJobName()));
        executor.getShellEnv().put(Constants.TASK_INDEX, String.valueOf(executor.getTaskIndex()));
        executor.getShellEnv().put(Constants.TASK_NUM, String.valueOf(executor.getNumTasks()));
        executor.getShellEnv().put(Constants.DISTRUBUTED_MODE_NAME, executor.getDistributedMode().name());
        if (executor.isGangMode()) {
            executor.getShellEnv().put(Constants.CLUSTER_SPEC, String.valueOf(executor.getClusterSpec()));
            executor.getShellEnv().put(
                    Constants.TF_CONFIG,
                    Utils.constructTFConfig(executor.getClusterSpec(), executor.getJobName(), executor.getTaskIndex())
            );
        }
    }
}