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

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.tony.Framework;
import com.linkedin.tony.TaskExecutor;

import static com.linkedin.tony.TonyConfigurationKeys.TENSORBOARD_LOG_DIR;

public class TestMLGenericRuntime {
    private TestRuntime runtime;

    static class TestRuntime extends MLGenericRuntime {
        @Override
        public Framework.TaskExecutorAdapter getTaskAdapter(TaskExecutor taskExecutor) {
            return new TestTaskExecutorAdapter(taskExecutor);
        }

        class TestTaskExecutorAdapter extends Task {

            public TestTaskExecutorAdapter(TaskExecutor executor) {
                super(executor);
            }

            @Override
            protected void buildTaskEnv(TaskExecutor executor) throws Exception {
                // ignore
            }
        }
    }

    @BeforeTest
    public void before() {
        runtime = new TestRuntime();
    }

    /**
     * Test MLGenericRuntime when in task executor.
     */
    @Test
    public void testNeedReserveTBPort() {
        TaskExecutor taskExecutor = new TaskExecutor();
        taskExecutor.setJobName("chief");
        Framework.TaskExecutorAdapter taskExecutorAdapter = runtime.getTaskAdapter(taskExecutor);

        taskExecutor.setChief(true);
        Assert.assertTrue(taskExecutorAdapter.needReserveTBPort());

        Configuration conf1 = new Configuration();
        conf1.set(TENSORBOARD_LOG_DIR, "/tmp");
        taskExecutor.setTonyConf(conf1);
        Assert.assertFalse(taskExecutorAdapter.needReserveTBPort());

        taskExecutor.setChief(false);
        Assert.assertFalse(taskExecutorAdapter.needReserveTBPort());

        taskExecutor.setJobName("tensorboard");
        Assert.assertTrue(taskExecutorAdapter.needReserveTBPort());
    }
}
