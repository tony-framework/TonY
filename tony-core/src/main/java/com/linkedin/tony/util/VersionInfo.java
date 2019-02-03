/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import static com.linkedin.tony.TonyConfigurationKeys.*;


/**
 * This class returns build information by reading the version-info.properties file.
 */
public class VersionInfo {
  private static final Log LOG = LogFactory.getLog(VersionInfo.class);

  private static final String VERSION_INFO_FILE = "version-info.properties";

  private Properties versionInfo;

  private VersionInfo() {
    versionInfo = new Properties();
    try (InputStream is = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(VERSION_INFO_FILE)) {
      if (is == null) {
        LOG.warn("Could not open input stream to " + VERSION_INFO_FILE);
      } else {
        versionInfo.load(is);
      }
    } catch (IOException ex) {
      LOG.warn("Could not read '" + VERSION_INFO_FILE + "', " + ex.toString(), ex);
    }
  }

  private String getVersionInternal() {
    return versionInfo.getProperty("version", "Unknown");
  }

  private String getRevisionInternal() {
    return versionInfo.getProperty("revision", "Unknown");
  }

  private String getBranchInternal() {
    return versionInfo.getProperty("branch", "Unknown");
  }

  private String getDateInternal() {
    return versionInfo.getProperty("date", "Unknown");
  }

  private String getUserInternal() {
    return versionInfo.getProperty("user", "Unknown");
  }

  private String getUrlInternal() {
    return versionInfo.getProperty("url", "Unknown");
  }

  private String getSrcChecksumInternal() {
    return versionInfo.getProperty("srcChecksum", "Unknown");
  }

  private static final VersionInfo COMMON_VERSION_INFO = new VersionInfo();

  /**
   * Get the jar version.
   * @return the version string, e.g. "0.1.5"
   */
  public static String getVersion() {
    return COMMON_VERSION_INFO.getVersionInternal();
  }

  /**
   * Get the commit hash for the root directory
   * @return the commit hash, e.g. "1cba10da369b846c53a3f99e9acbf8321db876e7"
   */
  public static String getRevision() {
    return COMMON_VERSION_INFO.getRevisionInternal();
  }

  /**
   * Get the branch on which this originated.
   * @return The branch name, e.g. "master" or "my-feature-branch"
   */
  public static String getBranch() {
    return COMMON_VERSION_INFO.getBranchInternal();
  }

  /**
   * The date this project was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return COMMON_VERSION_INFO.getDateInternal();
  }

  /**
   * The user that compiled this project.
   * @return the username of the user
   */
  public static String getUser() {
    return COMMON_VERSION_INFO.getUserInternal();
  }

  /**
   * Get the Git URL for the root directory.
   */
  public static String getUrl() {
    return COMMON_VERSION_INFO.getUrlInternal();
  }

  /**
   * Get the checksum of the source files from which this project was built.
   **/
  public static String getSrcChecksum() {
    return COMMON_VERSION_INFO.getSrcChecksumInternal();
  }

  public static void injectVersionInfo(Configuration conf) {
    conf.set(TONY_VERSION_INFO_VERSION, getVersion());
    conf.set(TONY_VERSION_INFO_REVISION, getRevision());
    conf.set(TONY_VERSION_INFO_BRANCH, getBranch());
    conf.set(TONY_VERSION_INFO_USER, getUser());
    conf.set(TONY_VERSION_INFO_DATE, getDate());
    conf.set(TONY_VERSION_INFO_URL, getUrl());
    conf.set(TONY_VERSION_INFO_CHECKSUM, getSrcChecksum());
  }

  public static void main(String[] args) {
    System.out.println("Version " + getVersion());
    System.out.println("Git " + getUrl() + " branch " + getBranch() + " revision " + getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("From source with checksum " + getSrcChecksum());
  }
}
