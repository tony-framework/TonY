/**
 * Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.horovod;

import java.io.IOException;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;

public class TestHorovodDriver {

    @BeforeClass
    public void beforeTest() {
        HorovodDriver.setInTest();
    }

    public void testHorovodDriver() throws IOException {
        String fakeWorkerList = "localhost:2";

        HorovodDriver driver = HorovodDriver.create(fakeWorkerList);
        Assert.assertNotEquals(HorovodDriver.getFakeServerPort(), driver.getPort());

        List<SlotInfo> slotInfoList = driver.getSlotInfoList();
        Assert.assertNotNull(slotInfoList);
        Assert.assertEquals(2, slotInfoList.size());
        Assert.assertEquals(0, slotInfoList.get(0).getCrossRank());
        Assert.assertEquals(1, slotInfoList.get(0).getCrossSize());
        Assert.assertEquals(0, slotInfoList.get(0).getLocalRank());
        Assert.assertEquals(2, slotInfoList.get(0).getLocalSize());
        Assert.assertEquals(0, slotInfoList.get(0).getRank());
        Assert.assertEquals(2, slotInfoList.get(0).getSize());

        driver.close();
    }
}
