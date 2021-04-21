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

import org.apache.hadoop.conf.Configuration;

import com.linkedin.tony.FrameworkRuntime;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.tensorflow.TonySession;

public class StandaloneRuntime implements FrameworkRuntime {
    @Override
    public String constructClusterSpec(String taskId) throws IOException {
        return taskId;
    }

    @Override
    public void destroy() {
        return;
    }

    @Override
    public void setTonySession(TonySession session) {
        return;
    }

    @Override
    public boolean canStartTask(TonyConfigurationKeys.DistributedMode distributedMode, String taskId) {
        return true;
    }

    @Override
    public boolean validateAndUpdateConfig(Configuration tonyConf) {
        return true;
    }

    @Override
    public boolean receiveTaskCallbackInfo(String taskId, String callbackInfo) {
        return true;
    }

    @Override
    public int run(TaskExecutor executor) throws Exception {
        return executorPythonShell(executor);
    }
}
