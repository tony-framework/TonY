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

import com.linkedin.tony.AbstractFrameworkRuntime;
import com.linkedin.tony.Framework;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.util.Utils;

public class StandaloneRuntime extends AbstractFrameworkRuntime {

    @Override
    public Framework.ApplicationMasterAdapter getAMAdapter() {
        return new AM();
    }

    @Override
    public Framework.TaskExecutorAdapter getTaskAdapter(TaskExecutor taskExecutor) {
        return new Task(taskExecutor);
    }

    @Override
    public String getFrameworkType() {
        return TonyConfigurationKeys.FrameworkType.STANDALONE.name();
    }

    class AM implements Framework.ApplicationMasterAdapter {

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
            if (Utils.getNumTotalTasks(tonyConf) != 1) {
                log.error("In standalone runtime mode, it must only have one instance.");
                return false;
            }
            return true;
        }

        @Override
        public boolean isHealthy(Configuration tonyConf) {
            return true;
        }

        @Override
        public boolean receiveTaskCallbackInfo(String taskId, String callbackInfo) {
            return true;
        }
    }

    static class Task implements Framework.TaskExecutorAdapter {
        private TaskExecutor executor;

        public Task(TaskExecutor taskExecutor) {
            this.executor = taskExecutor;
        }

        @Override
        public int run() throws Exception {
            assert executor != null;
            return executorPythonShell(executor);
        }

        @Override
        public boolean needReserveTBPort() {
            return false;
        }
    }
}
