/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.horovod;

import java.io.IOException;
import java.util.List;

import org.testng.Assert;

/**
 * @author zhangjunfan
 * @date 4/5/21
 */
public class TestHorovodDriver {

    public void testHorovodDriver() throws IOException {
        String fakeWorkerList = "localhost:2";

        HorovodDriver.setInTest();
        HorovodDriver driver = HorovodDriver.create(fakeWorkerList);
        Assert.assertNotEquals(-1, driver.getPort());

        List<SlotInfo> slotInfoList = driver.getSlotInfoList();
        Assert.assertNotNull(slotInfoList);
        Assert.assertEquals(2, slotInfoList.size());

        driver.close();
    }
}
