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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.linkedin.tony.Constants;
import com.linkedin.tony.Framework;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.util.Utils;

import static com.linkedin.tony.Constants.EVALUATOR_JOB_NAME;

public class TFRuntime extends MLGenericRuntime {

    @Override
    public Framework.TaskExecutorAdapter getTaskAdapter(TaskExecutor taskExecutor) {
        return new TFTaskExecutor(taskExecutor);
    }

    @Override
    public Framework.ApplicationMasterAdapter getAMAdapter() {
        return new TFApplicationMaster();
    }

    @Override
    public String getFrameworkType() {
        return TonyConfigurationKeys.FrameworkType.TENSORFLOW.name();
    }

    class TFTaskExecutor extends Task {

        public TFTaskExecutor(TaskExecutor executor) {
            super(executor);
        }

        @Override
        protected void buildTaskEnv(TaskExecutor executor) throws Exception {
            log.info("Setting up TensorFlow job...");
            Map<String, String> executorShellEnv = executor.getShellEnv();
            executorShellEnv.put(Constants.JOB_NAME, String.valueOf(executor.getJobName()));
            executorShellEnv.put(Constants.TASK_INDEX, String.valueOf(executor.getTaskIndex()));
            executorShellEnv.put(Constants.TASK_NUM, String.valueOf(executor.getNumTasks()));
            executorShellEnv.put(Constants.DISTRIBUTED_MODE_NAME, executor.getDistributedMode().name());
            if (executor.isGangMode()) {
                executor.getShellEnv().put(Constants.CLUSTER_SPEC, String.valueOf(executor.getClusterSpec()));
                executor.getShellEnv().put(
                        Constants.TF_CONFIG,
                        Utils.constructTFConfig(executor.getClusterSpec(), executor.getJobName(), executor.getTaskIndex())
                );
            }
        }
    }

    class TFApplicationMaster extends AM {
        private long evaluatorAloneStartTime = 0;

        @Override
        public boolean isHealthy(Configuration tonyConf) {
            if (!super.isHealthy(tonyConf)) {
                return false;
            }

            // If evaluator run alone and exceed timeout, job will fail and release resources.
            Map<String, Long> jobTypes = Utils.getRunAloneJobTypesWithTimeout(tonyConf);
            if (!jobTypes.containsKey(EVALUATOR_JOB_NAME)) {
                return true;
            }

            int notCompletedTrackedTasks = session.getTotalTrackedTasks() - session.getNumCompletedTrackedTasks();
            if (session.getNotCompletedTrackedTasks(EVALUATOR_JOB_NAME) == notCompletedTrackedTasks) {
                if (evaluatorAloneStartTime == 0) {
                    evaluatorAloneStartTime = System.currentTimeMillis();
                } else {
                    long timeout = jobTypes.get(EVALUATOR_JOB_NAME);
                    if (System.currentTimeMillis() - evaluatorAloneStartTime > timeout) {
                        Set<String> types = Utils.getSucceededOnRunAloneJobTimeout(tonyConf);
                        if (types.contains(EVALUATOR_JOB_NAME)) {
                            session.setFinalStatus(FinalApplicationStatus.SUCCEEDED,
                                    "Evaluator run alone and exceed timeout, make job success due to the conf of "
                                            + TonyConfigurationKeys.TASK_SUCCEEDED_ON_RUN_ALONE_TIMEOUT_JOBTYPES);
                        } else {
                            session.setFinalStatus(FinalApplicationStatus.FAILED,
                                    "Evaluator run alone and exceed timeout, make job failed");
                        }
                        return false;
                    }
                }
            }

            return true;
        }
    }
}
