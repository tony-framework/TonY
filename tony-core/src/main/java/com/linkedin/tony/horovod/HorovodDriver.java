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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linkedin.tony.util.Utils;

import static java.util.Objects.requireNonNull;

/**
 * Introduce HorovodDriver class to be a Horovod driver, which is similar to the role of Horovod launcher(gloo_run).
 * The responsibilities of the HorovodDriver are as follows
 * 1. Start the rendezvous server by using the built-in python script (horovod_driver.py in resource folder).
 * 2. Get the python script's process output and parse it, and pass it to Horovod runtime
 * 3. Monitor rendezvous server's python process
 *
 * Advanced mode(Refer to: https://github.com/linkedin/TonY/issues/536)
 * When enable driver debug mode, the Horovod driver can be started by using user's custom python script.
 * Worker list can be found from env'CLUSTER_WORKER_LIST'and output path can be found from
 * env 'DRIVER_OUTPUT_PATH'in python script.
 * And the script must touch a file after starting rendezvous server.
 * This file must be'{PORT}_____HOROVOD_RENDEZVOUS_SERVER____'as name and Horovod's slot info as content.
 */
public class HorovodDriver {
    private static final Log LOG = LogFactory.getLog(HorovodDriver.class);
    private static final Path DRIVER_SCRIPT_PATH = requireNonNull(createDriverScripPath());
    private static final String FAKE_SERVER_PORT = "9999";
    private static final String DRIVER_PYTHON_SCRIPT_NAME = "horovod_driver.py";
    private static final String DRIVER_TMP_FOLDER_NAME = "horovod_driver";
    private static final String CLUSTER_WORKER_LIST = "CLUSTER_WORKER_LIST";
    private static final String DRIVER_OUTPUT_PATH = "DRIVER_OUTPUT_PATH";
    public static final String PORT_FILE_NAME_SUFFIX = "____HOROVOD_RENDEZVOUS_SERVER____";

    // Just for unit test.
    private static boolean inTestMode = false;
    private static boolean failInTestMode = false;

    private Process taskProcess;
    private int port;
    private List<SlotInfo> slotInfoList;

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

    @VisibleForTesting
    protected static Path createDriverScripPath() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final String driverScript = DRIVER_PYTHON_SCRIPT_NAME;
        try {
            Path tempDir = Files.createTempDirectory(DRIVER_TMP_FOLDER_NAME);
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

    public synchronized static HorovodDriver create(String workerList, Map<String, String> shellEnv,
            String driverDebugCmd) throws Exception {
        reset();
        return startRendezvousServer(workerList, shellEnv, driverDebugCmd);
    }

    @VisibleForTesting
    protected static void reset() throws IOException {
        Path parentPath = getDriverOutputDir().toPath();

        if (!existPortFile()) {
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
     * @param taskProcess
     */
    private static Pair<Integer, List<SlotInfo>> waitTillServerStarted(final Process taskProcess) throws Exception {
        int checkCount = 0;
        int maxCheckCount = 5;
        Duration checkInterval = Duration.ofSeconds(2);

        while (!existPortFile() && (checkCount++) < maxCheckCount) {
            if (taskProcess != null && !taskProcess.isAlive()) {
                throw new Exception("Horovod Driver python process has finished, exit code: " + taskProcess.exitValue());
            }

            try {
                LOG.info("Rendezvous server don't start, sleep for " + checkInterval.getSeconds() + " secs");
                Thread.sleep(checkInterval.toMillis());
            } catch (Exception e) {
                LOG.warn(e);
            }
        }

        if (checkCount > maxCheckCount) {
            LOG.error("Timeout of starting horovod driver.");
            throw new Exception("Errors on starting horovod driver within the fixed time.");
        }

        if (taskProcess != null && !taskProcess.isAlive()) {
            String msg = "Driver python process has ended abnormally, exit code: " + taskProcess.exitValue();
            LOG.error(msg);
            throw new Exception(msg);
        }
        return getServerInfo();
    }

    @VisibleForTesting
    protected static Pair<Integer, List<SlotInfo>> getServerInfo() throws IOException {
        int port = -1;
        File[] files = getDriverOutputDir().listFiles();
        if (files == null) {
            return Pair.of(port, null);
        }

        for (File file : files) {
            String fileName = file.getName();
            LOG.info("List file: " + fileName);
            if (fileName.endsWith(PORT_FILE_NAME_SUFFIX)) {
                int tempIndex = fileName.indexOf(PORT_FILE_NAME_SUFFIX);
                port = Integer.parseInt(fileName.substring(0, tempIndex));
                String fileContent = FileUtils.readFileToString(file);
                // TODO: 4/10/21 fast fail when file content is empty.
                LOG.info("Horovod rendezvous server slot info: \n" + fileContent);
                List<SlotInfo> slotInfoList = new Gson().fromJson(fileContent,
                        new TypeToken<List<SlotInfo>>() { }.getType());
                return Pair.of(port, slotInfoList);
            }
        }
        LOG.info("Still no starting horovod rendezvous server.");
        return Pair.of(port, null);
    }

    private static boolean existPortFile() throws IOException {
        return getServerInfo().getLeft() != -1 ? true : false;
    }

    private static HorovodDriver startRendezvousServer(String workerlist, Map<String, String> shellEnv,
            String driverDebugProcessCmd) throws Exception {
        assert DRIVER_SCRIPT_PATH != null;

        String driverProcessCommand =
                driverDebugProcessCmd != null ? driverDebugProcessCmd : getBuildInDriverCMD(workerlist);

        ProcessBuilder taskProcessBuilder = new ProcessBuilder("bash", "-c", driverProcessCommand);
        taskProcessBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        taskProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        if (shellEnv != null) {
            shellEnv.put(CLUSTER_WORKER_LIST, workerlist);
            shellEnv.put(DRIVER_OUTPUT_PATH, getDriverOutputDir().getAbsolutePath());
            LOG.error("shell env: " + DRIVER_OUTPUT_PATH + "=" + shellEnv.get(DRIVER_OUTPUT_PATH));
            taskProcessBuilder.environment().putAll(shellEnv);
        }

        LOG.info("Starting python's Horovod driver cmd: " + driverProcessCommand);
        Process taskProcess = taskProcessBuilder.start();
        Pair<Integer, List<SlotInfo>> serverInfo = waitTillServerStarted(taskProcess);
        return new HorovodDriver(taskProcess, serverInfo.getLeft(), serverInfo.getRight());
    }

    @VisibleForTesting
    protected static File getDriverOutputDir() {
        assert DRIVER_SCRIPT_PATH != null;

        Path parentPath = DRIVER_SCRIPT_PATH.getParent();
        assert parentPath != null;

        File parentFile = parentPath.toFile();
        assert parentFile != null;

        return parentFile;
    }

    private static String getBuildInDriverCMD(String workerlist) {
        String driverProcessCommand = String.format("python %s -w %s", DRIVER_SCRIPT_PATH, workerlist);
        if (inTestMode) {
            driverProcessCommand += " -t " + " -p " + FAKE_SERVER_PORT;

            if (failInTestMode) {
                driverProcessCommand += " -f";
            }
        }
        return driverProcessCommand;
    }

    public void close() {
        if (taskProcess != null) {
            killProcess(taskProcess);
        }

        this.slotInfoList = null;
        this.port = -1;

        try {
            reset();
        } catch (IOException e) {
            LOG.error("Errors on cleaning up driver tmp files.", e);
        }
    }

    public int waitFor(long timeout) throws InterruptedException {
        if (timeout <= 0) {
            this.taskProcess.waitFor();
        } else {
            this.taskProcess.waitFor(timeout, TimeUnit.MICROSECONDS);
        }

        return this.taskProcess.exitValue();
    }

    public int waitFor() throws InterruptedException {
        return waitFor(-1);
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
        HorovodDriver.inTestMode = true;
    }

    public static String getFakeServerPort() {
        return FAKE_SERVER_PORT;
    }

    public static void setTaskFailInTestMode() {
        HorovodDriver.failInTestMode = true;
    }

    public static void removeTaskFailInTestMode() {
        HorovodDriver.failInTestMode = false;
    }

    public String getCallbackInfo() throws IOException {
        DriverCallbackInfo callbackInfo = new DriverCallbackInfo(String.valueOf(port),
                Utils.getCurrentHostName(), slotInfoList);
        return new ObjectMapper().writeValueAsString(callbackInfo);
    }

    public int getExitCode() {
        if (!taskProcess.isAlive()) {
            return taskProcess.exitValue();
        }

        LOG.error("Task process is still alive.");
        return -1;
    }
}
