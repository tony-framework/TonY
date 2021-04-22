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

import com.linkedin.tony.runtime.MXNetRuntime;
import com.linkedin.tony.runtime.PyTorchRuntime;
import com.linkedin.tony.runtime.StandaloneRuntime;
import com.linkedin.tony.runtime.TFRuntime;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.util.Utils;

public interface FrameworkRuntime {
    static FrameworkRuntime get(TonyConfigurationKeys.FrameworkType frameworkType) {
        switch (frameworkType) {
            case TENSORFLOW:
                return new TFRuntime();
            case PYTORCH:
                return new PyTorchRuntime();
            case MXNET:
                return new MXNetRuntime();
            case STANDALONE:
                return new StandaloneRuntime();
            default:
                throw new RuntimeException("Unsupported executor framework: " + frameworkType);
        }
    }

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
     * For AM, it will receive some callback info from task executor.
     * This method will be called when Application Master accepting task executors' callback info.
     * This method is suitable for the task executors that have a dependency of startup sequence,
     * and the start of downstream tasks needs to rely on the info after the start of the upstream task.
     */
    boolean receiveTaskCallbackInfo(String taskId, String callbackInfo);

    /** For Task Executor, execute task process **/
    int run(TaskExecutor executor) throws Exception;

    default int executorPythonShell(TaskExecutor executor) throws IOException, InterruptedException {
        return Utils.executeShell(executor.getTaskCommand(), executor.getTimeOut(), executor.getShellEnv());
    }
}
