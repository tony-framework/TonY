/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.tony.util.gpu;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Ported from hadoop 2.9.0 to allow GPU discovery
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GpuDiscoverer {
  public static final Log LOG = LogFactory.getLog(
      GpuDiscoverer.class);
  @VisibleForTesting
  protected static final String DEFAULT_BINARY_NAME = "nvidia-smi";

  // When executable path not set, try to search default dirs
  // By default search /usr/bin, /bin, and /usr/local/nvidia/bin (when
  // launched by nvidia-docker.
  private static final Set<String> DEFAULT_BINARY_SEARCH_DIRS = ImmutableSet.of(
      "/usr/bin", "/bin", "/usr/local/nvidia/bin");

  // command should not run more than 10 sec.
  private static final int MAX_EXEC_TIMEOUT_MS = 10 * 1000;
  private static GpuDiscoverer instance;

  static {
    instance = new GpuDiscoverer();
  }

  private Configuration conf = null;
  private String pathOfGpuBinary = null;
  private Map<String, String> environment = new HashMap<>();
  private GpuDeviceInformationParser parser = new GpuDeviceInformationParser();

  private int numOfErrorExecutionSinceLastSucceed = 0;
  GpuDeviceInformation lastDiscoveredGpuInformation = null;

  private void validateConfOrThrowException() throws GpuInfoException {
    if (conf == null) {
      throw new GpuInfoException("Please initialize (call initialize) before use "
          + GpuDiscoverer.class.getSimpleName());
    }
  }

  /**
   * Get GPU device information from system.
   * This need to be called after initialize.
   *
   * Please note that this only works on *NIX platform, so external caller
   * need to make sure this.
   *
   * @return GpuDeviceInformation
   * @throws GpuInfoException when any error happens
   */
  public synchronized GpuDeviceInformation getGpuDeviceInformation()
      throws GpuInfoException {
    validateConfOrThrowException();

    if (null == pathOfGpuBinary) {
      throw new GpuInfoException(
          "Failed to find GPU discovery executable, please double check "
              + TonyConfigurationKeys.GPU_PATH_TO_EXEC + " setting.");
    }

    if (numOfErrorExecutionSinceLastSucceed == Constants.MAX_REPEATED_GPU_ERROR_ALLOWED) {
      String msg =
          "Failed to execute GPU device information detection script for "
              + Constants.MAX_REPEATED_GPU_ERROR_ALLOWED
              + " times, skip following executions.";
      LOG.error(msg);
      throw new GpuInfoException(msg);
    }

    String output;
    try {
      output = Shell.execCommand(environment,
          new String[] { pathOfGpuBinary, "-x", "-q" }, MAX_EXEC_TIMEOUT_MS);
      GpuDeviceInformation info = parser.parseXml(output);
      numOfErrorExecutionSinceLastSucceed = 0;
      lastDiscoveredGpuInformation = info;
      return info;
    } catch (IOException e) {
      numOfErrorExecutionSinceLastSucceed++;
      String msg =
          "Failed to execute " + pathOfGpuBinary + " exception message:" + e
              .getMessage() + ", continue ...";
      if (LOG.isDebugEnabled()) {
        LOG.debug(msg);
      }
      throw new GpuInfoException(e);
    } catch (GpuInfoException e) {
      numOfErrorExecutionSinceLastSucceed++;
      String msg = "Failed to parse xml output" + e.getMessage();
      if (LOG.isDebugEnabled()) {
        LOG.warn(msg, e);
      }
      throw e;
    }
  }

  public synchronized GpuDeviceInformation initialize(Configuration conf) {
    GpuDeviceInformation info = null;
    this.conf = conf;
    numOfErrorExecutionSinceLastSucceed = 0;
    String pathToExecutable = conf.get(TonyConfigurationKeys.GPU_PATH_TO_EXEC,
        TonyConfigurationKeys.DEFAULT_GPU_PATH_TO_EXEC);
    if (pathToExecutable.isEmpty()) {
      pathToExecutable = DEFAULT_BINARY_NAME;
    }

    // Validate file existence
    File binaryPath = new File(pathToExecutable);

    if (!binaryPath.exists()) {
      LOG.warn("Failed to locate binary at: " + binaryPath.getAbsolutePath());
      // When binary not exist, use default setting.
      boolean found = false;
      for (String dir : DEFAULT_BINARY_SEARCH_DIRS) {
        binaryPath = new File(dir, DEFAULT_BINARY_NAME);
        if (binaryPath.exists()) {
          found = true;
          pathOfGpuBinary = binaryPath.getAbsolutePath();
          break;
        }
      }

      if (!found) {
        LOG.warn("Failed to locate binary at:" + binaryPath.getAbsolutePath()
            + ", please double check [" + TonyConfigurationKeys.GPU_PATH_TO_EXEC
            + "] setting. Now use " + "default binary:" + DEFAULT_BINARY_NAME);
      }
    } else {
      // If path specified by user is a directory, use
      if (binaryPath.isDirectory()) {
        binaryPath = new File(binaryPath, DEFAULT_BINARY_NAME);
        LOG.warn("Specified path is a directory, use " + DEFAULT_BINARY_NAME
            + " under the directory, updated path-to-executable:" + binaryPath
            .getAbsolutePath());
      }
      // Validated
      pathOfGpuBinary = binaryPath.getAbsolutePath();
    }

    // Try to discover GPU information once and print
    try {
      LOG.info("Trying to discover GPU information ...");
      info = getGpuDeviceInformation();
      LOG.info(info.toString());
    } catch (GpuInfoException e) {
      String msg =
          "Failed to discover GPU information from system, exception message:"
              + e.getMessage() + " continue...";
      LOG.warn(msg);
    }

    return info;
  }

  public int getNumOfErrorExecutionSinceLastSucceed() {
    return this.numOfErrorExecutionSinceLastSucceed;
  }

  @VisibleForTesting
  protected Map<String, String> getEnvironmentToRunCommand() {
    return environment;
  }

  @VisibleForTesting
  protected String getPathOfGpuBinary() {
    return pathOfGpuBinary;
  }

  public static GpuDiscoverer getInstance() {
    return instance;
  }
}
