/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.linkedin.tony.Constants;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.models.JobConfig;
import com.linkedin.tony.models.JobEvent;
import com.linkedin.tony.models.JobMetadata;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import static com.linkedin.tony.util.HdfsUtils.pathExists;


/**
 * The class handles parsing different file format
 */
public class ParserUtils {
  private static final Log LOG = LogFactory.getLog(ParserUtils.class);

  /**
   * Checks if {@code fileName} string contains valid metadata.
   * Ex: If {@code jobIdRegex} is "^application_\\d+_\\d+$",
   * {@code fileName} = "application_1541469337545_0031-1542325695566-1542325733637-user1-FAILED.jhist" -> true
   * {@code fileName} = "application_1541469337545_0031-1542325695566-user1.jhist.inprogress" -> true
   * {@code fileName} = "application_1541469337545_0031-1542325695566-1542325733637-user2-succeeded.jhist" -> false
   * because status should be upper-cased.
   * {@code fileName} = "job_01_01-1542325695566-1542325733637-user3-SUCCEEDED.jhist" -> false
   * because the job ID portion doesn't match {@code jobIdRegex}.
   * If a metadata component doesn't satisfy its corresponding condition, {@code fileName} is invalid.
   * @param fileName A string with metadata components.
   * @param jobIdRegex Regular expression string to validate metadata.
   * @return true if {@code fileName} are in proper format, false otherwise.
   */
  @VisibleForTesting
  public static boolean isValidHistFileName(String fileName, String jobIdRegex) {
    if (Strings.isNullOrEmpty(fileName)) {
      LOG.error("No filename provided!");
      return false;
    }

    String histFileNoExt = fileName.substring(0, fileName.indexOf('.'));
    String[] metadataArr = histFileNoExt.split("-");
    if (metadataArr.length < 3) {
      LOG.error("Missing fields in metadata");
      return false;
    }

    if (fileName.endsWith(Constants.INPROGRESS)) {
      return metadataArr[0].matches(jobIdRegex)
          && metadataArr[1].matches("\\d+")
          && metadataArr[2].matches(metadataArr[2].toLowerCase());
    }

    Preconditions.checkArgument(metadataArr.length == 5);
    return metadataArr[0].matches(jobIdRegex)
        && metadataArr[1].matches("\\d+")
        && metadataArr[2].matches("\\d+")
        && metadataArr[3].matches(metadataArr[3].toLowerCase())
        && metadataArr[4].equals(metadataArr[4].toUpperCase());
  }

  /**
   * Returns the full path of the latest (by start time) jhist file in {@code jobFolderPath}.
   * @param fs FileSystem object.
   * @param jobFolderPath Path of job directory.
   * @return the full path of the jhist file or {@code null} if an error occurs or no history file is found.
   */
  public static String getJhistFilePath(FileSystem fs, Path jobFolderPath) {
    try {
      // We want to have both running jobs and completed jobs
      // so we can't use endsWith() but rather contains() to filter
      List<String> histFilePaths = Arrays.stream(fs.listStatus(jobFolderPath))
          .filter(f -> f.getPath().toString().contains(Constants.HISTFILE_SUFFIX))
          .map(f -> f.getPath().toString()).collect(Collectors.toList());
      if (histFilePaths.isEmpty()) {
        LOG.warn("No history files found in " + jobFolderPath);
        return null;
      }

      // There may be multiple jhist files if there were multiple AM attempts.
      // We should use the one with the latest start time.
      histFilePaths.sort((filePath1, filePath2) -> {
        long startTime1 = Long.parseLong(HdfsUtils.getLastComponent(filePath1).split("-")[1]);
        long startTime2 = Long.parseLong(HdfsUtils.getLastComponent(filePath2).split("-")[1]);
        long difference = startTime1 - startTime2;
        if (difference < 0) {
          return -1;
        } else if (difference > 0) {
          return 1;
        } else {
          return 0;
        }
      });
      return histFilePaths.get(histFilePaths.size() - 1);
    } catch (IOException e) {
      LOG.error("Failed to scan " + jobFolderPath, e);
      return null;
    }
  }

  /**
   * Returns the completed time portion of a jhist file name as a {@link long}.
   * Assumes the jhist file name is valid and represents a completed job.
   */
  public static long getCompletedTimeFromJhistFileName(String jhistFileName) {
    if (jhistFileName.lastIndexOf("/") >= 0) {
      jhistFileName = jhistFileName.substring(jhistFileName.lastIndexOf("/") + 1);
    }
    return Long.parseLong(jhistFileName.split("-")[2]);
  }

  /**
   * Parses the latest (by start time) jhist file in {@code jobFolderPath} and returns a {@code JobMetadata} object
   * for the jhist file.
   * @param fs FileSystem object.
   * @param jobFolderPath Path object of job directory.
   * @param jobIdRegex Regular expression string to validate metadata.
   * @return a {@code JobMetadata} object or {@code null} if a jhist file could not be found or an error occurred
   * during processing.
   */
  public static JobMetadata parseMetadata(FileSystem fs, YarnConfiguration yarnConf, Path jobFolderPath, String jobIdRegex) {
    if (!pathExists(fs, jobFolderPath)) {
      return null;
    }

    String jhistFilePath = getJhistFilePath(fs, jobFolderPath);
    if (Strings.isNullOrEmpty(jhistFilePath)) {
      return null;
    }

    String histFileName = HdfsUtils.getLastComponent(jhistFilePath);
    if (!isValidHistFileName(histFileName, jobIdRegex)) {
      // this should never happen unless user rename the history file
      LOG.warn("Invalid history file name " + histFileName);
      return null;
    }

    LOG.debug("Successfully parsed metadata");
    return JobMetadata.newInstance(yarnConf, histFileName);
  }

  /**
   * Assuming that there's only 1 config file in {@code jobFolderPath},
   * this function parses the config file and returns a list of {@code JobConfig} objects.
   * @param fs FileSystem object.
   * @param jobFolderPath Path object of job directory.
   * @return a list of {@code JobConfig} objects.
   */
  public static List<JobConfig> parseConfig(FileSystem fs, Path jobFolderPath) {
    if (!pathExists(fs, jobFolderPath)) {
      return Collections.emptyList();
    }

    Path configFilePath = new Path(jobFolderPath, Constants.TONY_FINAL_XML);
    try {
      if (!fs.exists(configFilePath)) {
        // For backward-compatibility
        // Remove once everyone is using open-source tony-0.3.5+
        configFilePath = new Path(jobFolderPath, "config.xml");
      }
    } catch (IOException e) {
      LOG.error("Encountered exception while checking existence of " + configFilePath, e);
    }

    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    List<JobConfig> configs = new ArrayList<>();

    try (FSDataInputStream inStrm = fs.open(configFilePath)) {
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(inStrm);
      doc.getDocumentElement().normalize();

      NodeList properties = doc.getElementsByTagName("property");

      for (int i = 0; i < properties.getLength(); i++) {
        Node property = properties.item(i);
        if (property.getNodeType() == Node.ELEMENT_NODE) {
          Element p = (Element) property;
          JobConfig jobConf = new JobConfig();
          String name = getNodeElementText(p, "name");
          String value = getNodeElementText(p, "value");
          if (name == null || value == null) {
            LOG.warn("Found config with null name or value. Name = " + name + ", value = " + value);
            continue;
          }
          jobConf.setName(name);
          jobConf.setValue(value);
          String finalText = getNodeElementText(p, "final");
          if (finalText != null && finalText.equalsIgnoreCase("true")) {
            jobConf.setFinal(true);
          }
          jobConf.setSource(getNodeElementText(p, "source"));
          configs.add(jobConf);
        }
      }
    } catch (SAXException e) {
      LOG.error("Failed to parse config file", e);
      return Collections.emptyList();
    } catch (ParserConfigurationException e) {
      LOG.error("Failed to init XML parser", e);
      return Collections.emptyList();
    } catch (IOException e) {
      LOG.error("Failed to read config file", e);
      return Collections.emptyList();
    }
    LOG.debug("Successfully parsed config");
    return configs;
  }

  private static String getNodeElementText(Element node, String tagName) {
    if (node != null) {
      NodeList nodeList = node.getElementsByTagName(tagName);
      if (nodeList.getLength() > 0) {
        return nodeList.item(0).getTextContent();
      }
    }
    return null;
  }

  /**
   * Parses the newest (by start time) jhist file in {@code jobFolderPath}, and returns a list of {@link Event}s
   * @param fs FileSystem object.
   * @param jobFolderPath Path object of job directory.
   * @return a list of {@link Event} objects.
   */
  public static List<Event> parseEvents(FileSystem fs, Path jobFolderPath) {
    if (!pathExists(fs, jobFolderPath)) {
      return Collections.emptyList();
    }

    String jhistFile = getJhistFilePath(fs, jobFolderPath);
    if (Strings.isNullOrEmpty(jhistFile)) {
      return Collections.emptyList();
    }

    Path historyFile = new Path(jhistFile);
    List<Event> events = new ArrayList<>();
    try (InputStream in = fs.open(historyFile)) {
      DatumReader<Event> datumReader = new SpecificDatumReader<>(Event.class);
      try (DataFileStream<Event> avroFileStream = new DataFileStream<>(in, datumReader)) {
        Event record;
        while (avroFileStream.hasNext()) {
          record = avroFileStream.next(null);
          events.add(record);
        }
      } catch (IOException e) {
        LOG.error("Failed to read events from " + historyFile, e);
      }
    } catch (IOException e) {
      LOG.error("Failed to open history file", e);
    }
    return events;
  }

  public static List<JobEvent> mapEventToJobEvent(List<Event> events) {
    return events.stream().map(JobEvent::convertEventToJobEvent).collect(Collectors.toList());
  }

  /**
   * Given a {@link Date} and {@link ZoneId}, returns a "yyyy/mm/dd" string representation in the time zone specified.
   */
  public static String getYearMonthDayDirectory(Date date, ZoneId zoneId) {
    StringBuilder dirString = new StringBuilder();
    LocalDate localDate = date.toInstant().atZone(zoneId).toLocalDate();
    dirString.append(localDate.getYear());
    dirString.append(Path.SEPARATOR).append(String.format("%02d", localDate.getMonthValue()));
    dirString.append(Path.SEPARATOR).append(String.format("%02d", localDate.getDayOfMonth()));
    return dirString.toString();
  }

  private ParserUtils() { }
}
