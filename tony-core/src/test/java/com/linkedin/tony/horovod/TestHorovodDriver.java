/**
 * Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.horovod;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestHorovodDriver {

    @BeforeClass
    public void beforeTest() {
        HorovodDriver.setInTest();
    }

    /**
     * It should start horovod driver successfully.
     * @throws Exception
     */
    @Test
    public void testHorovodDriver() throws Exception {
        // Fake worker list is useless, when in test mode,
        // python code will return the "localhost:2" host plan
        String fakeWorkerList = "localhost:2";

        HorovodDriver driver = null;
        try {
            driver = HorovodDriver.create(fakeWorkerList, null, null);
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
        } finally {
            if (driver != null) {
                driver.close();
            }
        }
    }

    /**
     * When python process exit, it will throw exception.
     */
    @Test
    public void testHorovodDriverWhenFailed() {
        HorovodDriver driver = null;
        try {
            HorovodDriver.setTaskFailInTestMode();
            String fakeWorkerList = "localhost:2";
            driver = HorovodDriver.create(fakeWorkerList, null, null);
            Assert.fail("Should throw exception on starting driver.");
        } catch (Exception e) {
            // ignore.
        } finally {
            if (driver != null) {
                driver.close();
            }
            HorovodDriver.removeTaskFailInTestMode();
        }
    }

    @Test
    public void testCreateDriverScripPath() {
        Path driverPath = HorovodDriver.createDriverScripPath();
        File driverFile = driverPath.toFile();
        Assert.assertNotNull(driverFile);
        Assert.assertTrue(driverFile.isFile());

        cleanupTmpFile(driverFile.getParentFile());
    }

    /**
     * Test get rendezvous server info by reading specific file.
     */
    @Test
    public void testGetServerInfo() throws IOException {
        Pair<Integer, List<SlotInfo>> infoPair = HorovodDriver.getServerInfo();
        Assert.assertNotNull(infoPair);
        Assert.assertEquals(-1, infoPair.getLeft().intValue());
        Assert.assertNull(infoPair.getRight());

        Path parentPath = HorovodDriver.getDriverOutputDir().toPath();

        // inject server info into files.
        int port = 10000;
        List<SlotInfo> slotInfos = buildFakeSlotInfo();
        ObjectMapper mapper = new ObjectMapper();
        String slotJson = mapper.writeValueAsString(slotInfos);
        // create tmp port file.
        createPortTmpFile(parentPath, port, slotJson);

        infoPair = HorovodDriver.getServerInfo();
        Assert.assertNotNull(infoPair);
        Assert.assertEquals(port, infoPair.getLeft().intValue());
        List<SlotInfo> metaSlotInfos = infoPair.getRight();
        Assert.assertNotNull(metaSlotInfos);

        String metaInfoJson = mapper.writeValueAsString(metaSlotInfos);
        Assert.assertNotNull(metaInfoJson);
        Assert.assertEquals(slotJson, metaInfoJson);

        // clear up all folders.
        cleanupTmpFile(parentPath.toFile());
    }

    private List<SlotInfo> buildFakeSlotInfo() {
        List<SlotInfo> slotInfos = new ArrayList<>();
        slotInfos.add(
                new SlotInfo("localhost", 0, 0, 0, 2, 2, 1)
        );
        slotInfos.add(
                new SlotInfo("localhost", 1, 1, 1, 2, 2, 1)
        );
        return slotInfos;
    }

    private void createPortTmpFile(Path parentPath, int port, String slotJson) throws IOException {
        String portFileName = String.format("%d%s", port, HorovodDriver.PORT_FILE_NAME_SUFFIX);
        File tmpFile = Paths.get(parentPath.toAbsolutePath().toString(), portFileName).toFile();
        FileUtils.writeStringToFile(tmpFile, slotJson);
    }

    private void cleanupTmpFile(File file) {
        if (file.isDirectory()) {
            File[] childfiles = file.listFiles();
            Arrays.stream(childfiles).filter(x -> !x.getName().endsWith(".py")).forEach(this::cleanupTmpFile);
        }
        boolean ok = file.delete();
        System.out.println(ok);
    }
}
