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

import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonyConfigurationKeys;

import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_GPUS;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_MEMORY;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_VCORE;
import static com.linkedin.tony.TonyConfigurationKeys.TB_GPUS;
import static com.linkedin.tony.TonyConfigurationKeys.TB_INSTANCES;
import static com.linkedin.tony.TonyConfigurationKeys.TB_MEMORY;
import static com.linkedin.tony.TonyConfigurationKeys.TB_VCORE;
import static com.linkedin.tony.TonyConfigurationKeys.TENSORBOARD_LOG_DIR;

public class TestMLGenericRuntime {
    private MLGenericRuntime runtime;

    static class TestRuntime extends MLGenericRuntime {
        @Override
        protected void buildTaskEnv(TaskExecutor executor) throws Exception {
            return;
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

        runtime.initTaskExecutorResource(taskExecutor);

        taskExecutor.setChief(true);
        Assert.assertTrue(runtime.needReserveTBPort());

        Configuration conf1 = new Configuration();
        conf1.set(TENSORBOARD_LOG_DIR, "/tmp");
        taskExecutor.setTonyConf(conf1);
        Assert.assertFalse(runtime.needReserveTBPort());

        taskExecutor.setChief(false);
        Assert.assertFalse(runtime.needReserveTBPort());

        taskExecutor.setJobName("tensorboard");
        Assert.assertTrue(runtime.needReserveTBPort());
    }

    @Test
    public void testCheckAndPrepareTBResource() {
        Configuration conf = new Configuration(false);
        Assert.assertTrue(runtime.checkAndPrepareTBResource(conf));

        conf.set(TonyConfigurationKeys.TENSORBOARD_LOG_DIR, "hdfs://tmp");
        Assert.assertTrue(runtime.checkAndPrepareTBResource(conf));
        Assert.assertEquals(conf.get(TB_INSTANCES), "1");
        Assert.assertEquals(conf.get(TB_VCORE), String.valueOf(DEFAULT_TB_VCORE));
        Assert.assertEquals(conf.get(TB_MEMORY), DEFAULT_TB_MEMORY);
        Assert.assertEquals(conf.get(TB_GPUS), String.valueOf(DEFAULT_TB_GPUS));

        conf = new Configuration(false);
        conf.set(TENSORBOARD_LOG_DIR, "hdfs://tmp");
        conf.set(TB_INSTANCES, String.valueOf(2));
        Assert.assertFalse(runtime.checkAndPrepareTBResource(conf));
    }
}
