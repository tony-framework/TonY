/**
 * Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.horovod;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import static java.util.Objects.requireNonNull;

public class HorovodDriver {
    private static final Log LOG = LogFactory.getLog(HorovodDriver.class);
    private static final Path DRIVER_SCRIPT_PATH = requireNonNull(createDriverScripPath());
    private static final String PORT_FILE_NAME_SUFFIX = "____HOROVOD_RENDEZVOUS_SERVER____";
    private static boolean isUT = false;

    private final Process taskProcess;
    private final int port;
    private final List<SlotInfo> slotInfoList;

    private HorovodDriver(Process taskProcess, int port, List<SlotInfo> slotInfos) {
        this.taskProcess = taskProcess;
        this.port = port;
        this.slotInfoList = slotInfos;
    }

    public List<SlotInfo> getSlotInfoList() {
        return slotInfoList;
    }

    public int getPort() {
        return port;
    }

    private static Path createDriverScripPath() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final String driverScript = "horovod_driver.py";
        try {
            Path tempDir = Files.createTempDirectory("horovod_driver");
            tempDir.toFile().deleteOnExit();
            try (InputStream stream = loader.getResourceAsStream(driverScript)) {
                Files.copy(stream, Paths.get(tempDir.toAbsolutePath().toString(), driverScript));
            }
            return Paths.get(tempDir.toAbsolutePath().toString(), driverScript);
        } catch (Exception e) {
            LOG.info(e);
            return null;
        }
    }

    public synchronized static HorovodDriver create(String workerList) throws IOException {
        reset();
        return startRendezvousServer(workerList);
    }

    @VisibleForTesting
    protected static void reset() throws IOException {
        // remove existed port files.
        assert DRIVER_SCRIPT_PATH != null;
        Path parentPath = DRIVER_SCRIPT_PATH.getParent();
        assert parentPath != null;

        if (!existPortFile(parentPath)) {
            return;
        }

        File[] files = parentPath.toFile().listFiles();
        if (files == null) {
            return;
        }

        Arrays.stream(files).filter(file -> file.getName().endsWith(PORT_FILE_NAME_SUFFIX))
                .forEach(file -> file.delete());
    }

    /**
     * @return Pair, left is Rendezvous server port, right is SlotInfo.
     * @throws IOException
     */
    private static Pair<Integer, List<SlotInfo>> waitTillServerStarted() throws IOException {
        assert DRIVER_SCRIPT_PATH != null;
        Path parentPath = DRIVER_SCRIPT_PATH.getParent();
        assert parentPath != null;

        int checkCount = 0;
        int maxCheckCount = 5;
        Duration checkInterval = Duration.ofSeconds(2);

        while (!existPortFile(parentPath) && (checkCount++) < maxCheckCount) {
            try {
                LOG.info("Rendezvous server don't start, sleep for " + checkInterval.getSeconds() + " secs");
                Thread.sleep(checkInterval.toMillis());
            } catch (Exception e) {
                LOG.warn(e);
            }
        }
        return getServerInfo(parentPath);
    }

    private static Pair<Integer, List<SlotInfo>> getServerInfo(Path parentPath) throws IOException {
        int port = -1;
        File parentFile = parentPath.toFile();
        requireNonNull(parentFile);
        File[] files = parentFile.listFiles();
        if (files == null) {
            return Pair.of(port, null);
        }

        for (File file : files) {
            String fileName = file.getName();
            if (fileName.endsWith(PORT_FILE_NAME_SUFFIX)) {
                int tempIndex = fileName.indexOf(PORT_FILE_NAME_SUFFIX);
                port = Integer.parseInt(fileName.substring(0, tempIndex));
                String fileContent = FileUtils.readFileToString(file);
                LOG.info("Horovod rendezvous server slot info: \n" + fileContent);
                List<SlotInfo> slotInfoList = new Gson().fromJson(fileContent,
                        new TypeToken<List<SlotInfo>>() { }.getType());
                return Pair.of(port, slotInfoList);
            }
        }
        LOG.info("Still no starting horovod rendezvous server.");
        return Pair.of(port, null);
    }

    private static boolean existPortFile(Path parentPath) throws IOException {
        return getServerInfo(parentPath).getLeft() != -1 ? true : false;
    }

    private static HorovodDriver startRendezvousServer(String workerlist) throws IOException {
        String driverProcess = String.format("python3 %s -w %s", DRIVER_SCRIPT_PATH, workerlist);
        if (isUT) {
            driverProcess += " -t";
        }

        ProcessBuilder taskProcessBuilder = new ProcessBuilder("bash", "-c", driverProcess);
        taskProcessBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        taskProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        LOG.info("Starting python's Horovod driver cmd: " + driverProcess);
        Process taskProcess = taskProcessBuilder.start();
        Pair<Integer, List<SlotInfo>> serverInfo = waitTillServerStarted();
        return new HorovodDriver(taskProcess, serverInfo.getLeft(), serverInfo.getRight());
    }

    public void close() {
        if (taskProcess != null) {
            killProcess(taskProcess);
        }
    }

    private static void killProcess(Process taskProcess) {
        if (!taskProcess.isAlive()) {
            return;
        }

        LOG.info("Killing the Horovod driver python process..");
        taskProcess.destroy();

        int checkCount = 0;
        int maxCheckCount = 10;
        while (taskProcess.isAlive() && (checkCount++) < maxCheckCount) {
            try {
                Thread.sleep(Duration.ofSeconds(1).toMillis());
            } catch (InterruptedException e) {
                LOG.info(e);
            }
        }

        if (taskProcess.isAlive()) {
            LOG.info("Killing the Horovod driver python process forcibly...");
            taskProcess.destroyForcibly();
        }

        LOG.info("Successfully killed the Horovod driver python process");
    }

    public static void setInTest() {
        HorovodDriver.isUT = true;
    }
}
