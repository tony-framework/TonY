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

public class PyTorchRuntime extends MLGenericRuntime {

    @Override
    public void buildTaskEnv(TaskExecutor executor) throws Exception {
        log.info("Setting up PyTorch job...");
        String initMethod = Utils.parseClusterSpecForPytorch(executor.getClusterSpec());
        if (initMethod == null) {
            throw new RuntimeException("Errors on getting init method.");
        }
        log.info("Init method is: " + initMethod);
        Map<String, String> executorShellEnv = executor.getShellEnv();
        executorShellEnv.put(Constants.INIT_METHOD, initMethod);
        executorShellEnv.put(Constants.RANK, String.valueOf(executor.getTaskIndex()));
        executorShellEnv.put(Constants.WORLD, String.valueOf(executor.getNumTasks()));
    }
}
