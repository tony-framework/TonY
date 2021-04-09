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

public class PyTorchRuntime implements MLFrameworkRuntime {
    private static final Log LOG = LogFactory.getLog(PyTorchRuntime.class);

    @Override
    public String getClusterSpec(TonySession session, String taskId) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(session.getClusterSpec());
    }

    @Override
    public void destory() {
        // ignore
    }

    @Override
    public void setEnv(TaskExecutor executor) throws Exception {
        LOG.info("Setting up PyTorch job...");
        String initMethod = Utils.parseClusterSpecForPytorch(executor.getClusterSpec());
        if (initMethod == null) {
            throw new RuntimeException("Errors on getting init method.");
        }
        LOG.info("Init method is: " + initMethod);
        executor.getShellEnv().put(Constants.INIT_METHOD, initMethod);
        executor.getShellEnv().put(Constants.RANK, String.valueOf(executor.getTaskIndex()));
        executor.getShellEnv().put(Constants.WORLD, String.valueOf(executor.getNumTasks()));
    }
}
