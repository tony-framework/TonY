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
package com.linkedin.tony;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.util.Utils;

/**
 * Providing a generic interface for both AM and Task executor in this Framework class.
 * To make more clearer, let the AM and Task executors' interface separate.
 * And any runtime including Horovod/Tensorflow runtime must implement the following
 * interface.
 */
public class Framework {

    public interface ApplicationMasterAdapter {
        /** For AM, getting cluster spec and return to task exectuor **/
        String constructClusterSpec(String taskId) throws IOException;

        /** For AM, when app finished, it need to call it to release resource **/
        void destroy();

        /** For AM, init the tony session **/
        void setTonySession(final TonySession session);

        /** For AM, it ensures that each task executor start sequence. like Horovod driver should start before workers **/
        boolean canStartTask(TonyConfigurationKeys.DistributedMode distributedMode, String taskId);

        /** For AM, it will pre-check tony conf and inject some params. like horovod runtime will inject driver config into it. **/
        boolean validateAndUpdateConfig(Configuration tonyConf);

        /**
         * For AM, it will check the runtime healthy periodically. If not ok, the job will fast fail.
         */
        boolean checkHealthy(Configuration tonyConf);

        /**
         * For AM, it will receive some callback info from task executor.
         * This method will be called when Application Master accepting task executors' callback info.
         * This method is suitable for the task executors that have a dependency of startup sequence,
         * and the start of downstream tasks needs to rely on the info after the start of the upstream task.
         */
        boolean receiveTaskCallbackInfo(String taskId, String callbackInfo);
    }

    public interface TaskExecutorAdapter {

        int run() throws Exception;

        boolean needReserveTBPort();

        default int executorPythonShell(TaskExecutor executor) throws IOException, InterruptedException {
            return Utils.executeShell(executor.getTaskCommand(), executor.getTimeOut(), executor.getShellEnv());
        }
    }
}
