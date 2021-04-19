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

import java.util.Map;

import com.linkedin.tony.Constants;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.util.Utils;

public class TFRuntime extends MLGenericRuntime {

    @Override
    public void buildTaskEnv(TaskExecutor executor) {
        log.info("Setting up TensorFlow job...");
        Map<String, String> executorShellEnv = executor.getShellEnv();
        executorShellEnv.put(Constants.JOB_NAME, String.valueOf(executor.getJobName()));
        executorShellEnv.put(Constants.TASK_INDEX, String.valueOf(executor.getTaskIndex()));
        executorShellEnv.put(Constants.TASK_NUM, String.valueOf(executor.getNumTasks()));
        executorShellEnv.put(Constants.DISTRUBUTED_MODE_NAME, executor.getDistributedMode().name());
        if (executor.isGangMode()) {
            executor.getShellEnv().put(Constants.CLUSTER_SPEC, String.valueOf(executor.getClusterSpec()));
            executor.getShellEnv().put(
                    Constants.TF_CONFIG,
                    Utils.constructTFConfig(executor.getClusterSpec(), executor.getJobName(), executor.getTaskIndex())
            );
        }
    }
}
