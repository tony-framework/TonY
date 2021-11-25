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
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.tony.Constants;
import com.linkedin.tony.Framework;
import com.linkedin.tony.TaskExecutor;
import com.linkedin.tony.TonySession;

import static com.linkedin.tony.TonyConfigurationKeys.TENSORBOARD_LOG_DIR;

public class TestMLGenericRuntime {
    private TestRuntime runtime;

    static class TestRuntime extends MLGenericRuntime {
        @Override
        public Framework.TaskExecutorAdapter getTaskAdapter(TaskExecutor taskExecutor) {
            return new TestTaskExecutorAdapter(taskExecutor);
        }

        @Override
        public Framework.ApplicationMasterAdapter getAMAdapter() {
            return new AM();
        }

        @Override
        public String getFrameworkType() {
            return null;
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

    /**
     * When no specifing dependencies, it will always return null.
     */
    @Test
    public void testGroupDependencyNoConfShouldPass() {
        Configuration conf = new Configuration();
        conf.addResource("tony-default.xml");
        conf.set("tony.application.dependency.A.timeout.after.B", "3600");
        conf.set("tony.application.dependency.B.timeout.after.C", "3600");

        TonySession session = buildMockSession(conf);
        MLGenericRuntime.AM am = (MLGenericRuntime.AM) runtime.getAMAdapter();
        am.setTonySession(session);
        Assert.assertNull(
                am.groupDependencyTimeout(conf)
        );
    }

    @Test
    public void testGroupDependencyShouldPass() {
        Configuration conf = new Configuration();
        conf.addResource("tony-default.xml");
        conf.set("tony.application.group.A", "worker,chief");
        conf.set("tony.application.group.B", "evaluator");
        conf.set("tony.application.dependency.B.timeout.after.A", "3600");

        TonySession session = buildMockSession(conf);
        TonySession.TonyTask chiefTask = session.getTask("chief", "0");
        chiefTask.setEndTime(System.currentTimeMillis() - 1000 * 60 * 120);

        MLGenericRuntime.AM am = (MLGenericRuntime.AM) runtime.getAMAdapter();
        am.setTonySession(session);
        Assert.assertEquals(
                am.groupDependencyTimeout(conf),
                "Jobtype: evaluator in group: B runs exceeded timeout due it's dependent "
                        + "jobtype: chief in group: A has been finished."
        );
    }

    @Test
    public void testGroupDependencyWorkerWhenChiefFinished() {
        Configuration conf = new Configuration();
        conf.addResource("tony-default.xml");
        conf.set("tony.application.group.A", "chief");
        conf.set("tony.application.group.B", "otherWorker");
        conf.set("tony.application.dependency.B.timeout.after.A", "3600");

        TonySession session = buildMockSession(conf);
        TonySession.TonyTask chiefTask = session.getTask("chief", "0");
        chiefTask.setEndTime(System.currentTimeMillis() - 1000 * 60 * 120);

        MLGenericRuntime.AM am = (MLGenericRuntime.AM) runtime.getAMAdapter();
        am.setTonySession(session);
        Assert.assertEquals(
                am.groupDependencyTimeout(conf),
                "Jobtype: otherWorker in group: B runs exceeded timeout due it's dependent jobtype: chief in group: A has been finished."
        );
    }

    @Test
    public void testGroupDependencyWithMultipleGroup() {
        Configuration conf = new Configuration();
        conf.addResource("tony-default.xml");
        conf.set("tony.application.group.A", "chief");
        conf.set("tony.application.group.B", "otherWorker");
        conf.set("tony.application.dependency.B.timeout.after.A", String.valueOf(60 * 240));

        conf.set("tony.application.group.C", "chief");
        conf.set("tony.application.group.D", "otherWorker");
        conf.set("tony.application.dependency.D.timeout.after.C", "3600");

        TonySession session = buildMockSession(conf);
        TonySession.TonyTask chiefTask = session.getTask("chief", "0");
        chiefTask.setEndTime(System.currentTimeMillis() - 1000 * 60 * 120);

        MLGenericRuntime.AM am = (MLGenericRuntime.AM) runtime.getAMAdapter();
        am.setTonySession(session);
        Assert.assertEquals(
                am.groupDependencyTimeout(conf),
                "Jobtype: otherWorker in group: D runs exceeded timeout due it's dependent jobtype: chief in group: C has been finished."
        );
    }

    @Test
    public void testGroupDependencyWithoutTimeoutMultipleGroup() {
        Configuration conf = new Configuration();
        conf.addResource("tony-default.xml");
        conf.set("tony.application.group.A", "chief");
        conf.set("tony.application.group.B", "otherWorker");
        conf.set("tony.application.dependency.B.timeout.after.A", String.valueOf(60 * 240));

        TonySession session = buildMockSession(conf);
        TonySession.TonyTask chiefTask = session.getTask("chief", "0");
        chiefTask.setEndTime(System.currentTimeMillis() - 1000 * 60 * 120);

        MLGenericRuntime.AM am = (MLGenericRuntime.AM) runtime.getAMAdapter();
        am.setTonySession(session);
        Assert.assertNull(
                am.groupDependencyTimeout(conf)
        );
    }

    private TonySession buildMockSession(Configuration tonyConf) {
        TonySession session = new TonySession.Builder().setTonyConf(tonyConf).build();

        TonySession.TonyTask ps0 = session.buildTonyTask(Constants.PS_JOB_NAME, "0", "localhost");
        TonySession.TonyTask ps1 = session.buildTonyTask(Constants.PS_JOB_NAME, "1", "localhost");

        TonySession.TonyTask chief = session.buildTonyTask(Constants.CHIEF_JOB_NAME, "0", "localhost");

        TonySession.TonyTask worker0 = session.buildTonyTask(Constants.WORKER_JOB_NAME, "0", "localhost");
        TonySession.TonyTask worker1 = session.buildTonyTask(Constants.WORKER_JOB_NAME, "1", "localhost");
        TonySession.TonyTask worker2 = session.buildTonyTask(Constants.WORKER_JOB_NAME, "2", "localhost");

        TonySession.TonyTask otherWorker0 = session.buildTonyTask("otherWorker", "0", "localhost");

        TonySession.TonyTask evaluator0 = session.buildTonyTask(Constants.EVALUATOR_JOB_NAME, "0", "localhost");

        ps0.setTaskInfo();
        ps1.setTaskInfo();
        chief.setTaskInfo();
        worker0.setTaskInfo();
        worker1.setTaskInfo();
        worker2.setTaskInfo();
        evaluator0.setTaskInfo();
        otherWorker0.setTaskInfo();

        session.addTask(ps0);
        session.addTask(ps1);
        session.addTask(chief);
        session.addTask(worker0);
        session.addTask(worker1);
        session.addTask(worker2);
        session.addTask(evaluator0);
        session.addTask(otherWorker0);

        chief.setExitStatus(ContainerExitStatus.SUCCESS);
        worker0.setExitStatus(ContainerExitStatus.SUCCESS);
        worker1.setExitStatus(ContainerExitStatus.SUCCESS);
        worker2.setExitStatus(ContainerExitStatus.SUCCESS);

        return session;
    }
}
