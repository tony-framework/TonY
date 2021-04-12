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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.tony.Constants;
import com.linkedin.tony.MLFrameworkRuntime;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.util.Utils;

public class MXNetRuntime implements MLFrameworkRuntime {
    private static final Log LOG = LogFactory.getLog(MXNetRuntime.class);

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
        LOG.info("Setting up MXNet job...");
        String[] dmlcServer = Utils.parseClusterSpecForMXNet(executor.getClusterSpec());
        if (dmlcServer == null) {
            throw new RuntimeException("Errors on getting dmlc server.");
        }
        int numServer = executor.getTonyConf().getInt(TonyConfigurationKeys.getInstancesKey(Constants.SERVER_JOB_NAME), 0);
        int numWorker = executor.getTonyConf().getInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME), 0);
        LOG.info("init DMLC is: " + dmlcServer[0] + " port: " + dmlcServer[1]);
        LOG.info("init DMLC ROLE: " + executor.getJobName());
        LOG.info("init DMLC NUM_PS: " + numServer);
        LOG.info("init DMLC NUM_WORKER: " + numWorker);

        Map<String, String> shellEnv = executor.getShellEnv();
        shellEnv.put(Constants.DMLC_ROLE, executor.getJobName());
        shellEnv.put(Constants.DMLC_PS_ROOT_URI, dmlcServer[0]);
        shellEnv.put(Constants.DMLC_PS_ROOT_PORT, dmlcServer[1]);
        shellEnv.put("DMLC_LOCAL", "0");
        //executor.shellEnv.put("DMLC_USE_KUBERNETES", "0");
        shellEnv.put(Constants.DMLC_NUM_SERVER, String.valueOf(numServer));
        shellEnv.put(Constants.DMLC_NUM_WORKER, String.valueOf(numWorker));
        //executor.shellEnv.put(Constants.PS_VERBOSE, "2");
    }
}
