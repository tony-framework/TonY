/**
 * Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.runtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.tony.TonySession;

public class TestHorovodRuntime {
    private TonySession session = new TonySession();
    private HorovodRuntime.HorovodAMAdapter amAdapter;

    @BeforeTest
    public void before() {
        Map<String, TonySession.TonyTask[]> taskMaps = session.getTonyTasks();

        List<TonySession.TonyTask> taskList = Arrays.asList(
                session.buildTonyTask("worker", "0", "localhost1"),
                session.buildTonyTask("worker", "1", "localhost1"),
                session.buildTonyTask("worker", "2", "localhost2"),
                session.buildTonyTask("worker", "3", "localhost3"),
                session.buildTonyTask("driver", "0", "localhost4")
                );

        taskMaps.put("worker", taskList.toArray(new TonySession.TonyTask[0]));

        amAdapter = (HorovodRuntime.HorovodAMAdapter) new HorovodRuntime().getAMAdapter();
    }

    @Test
    public void testBuildWorkerList() {
        List<Integer> sameHostIndexCollection = new ArrayList<>();
        String currenthost = "localhost1";
        String workerList = amAdapter.buildWorkerList(session, currenthost, sameHostIndexCollection);
        Assert.assertEquals("localhost3:1,localhost2:1,localhost1:2", workerList);
        Assert.assertEquals(2, sameHostIndexCollection.size());
        Assert.assertEquals("[0, 1]", sameHostIndexCollection.toString());

        sameHostIndexCollection = new ArrayList<>();
        currenthost = "localhost2";
        workerList = amAdapter.buildWorkerList(session, currenthost, sameHostIndexCollection);
        Assert.assertEquals("localhost3:1,localhost2:1,localhost1:2", workerList);
        Assert.assertEquals(1, sameHostIndexCollection.size());
        Assert.assertEquals("[2]", sameHostIndexCollection.toString());
    }

    @Test
    public void testValidate() {
        Configuration tonyConf = new Configuration();
        tonyConf.set("tony.application.untracked.jobtypes", "worker");
        tonyConf.set("tony.driver.instances", "2");
        tonyConf.set("tony.driver.vcores", "2");
        Assert.assertFalse(amAdapter.validateAndUpdateConfig(tonyConf));

        tonyConf = new Configuration();
        Assert.assertTrue(amAdapter.validateAndUpdateConfig(tonyConf));
    }
}
