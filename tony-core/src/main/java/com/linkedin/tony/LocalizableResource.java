/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Parse container resources of format:
 * SOURCE_FILE_PATH::PATH_IN_CONTAINER#archive
 * only SOURCE_FILE_PATH is required.
 *
 * SOURCE_FILE_PATH: location of the file to be localized to containers. This could be either local resources or remote
 *                   resources.
 * PATH_IN_CONTAINER: optional, default to source file name. If specified, source_file_path will be localized as name
 *                   file_in_container in container.
 * ARCHIVE: if #archive is put at the end, the file will be uploaded as ARCHIVE type and unzipped upon localization.
 */
public class LocalizableResource {

  /**
   * The complete resource string which contains annotations like #archive or
   * ::abc
   */
  private String completeResourceString;

  private boolean isDirectory;
  private LocalResourceType resourceType;
  private FileStatus sourceFileStatus;

  // The path of the source file.
  private Path sourceFilePath;

  // The file name of this resource inside the container.
  private String localizedFileName;

  private LocalizableResource() { }

  public boolean isDirectory() {
    return isDirectory;
  }

  public boolean isLocalFile() {
    return new Path(completeResourceString).toUri().getScheme() == null;
  }

  public boolean isArchive() {
    return resourceType == LocalResourceType.ARCHIVE;
  }

  public Path getSourceFilePath() {
    return sourceFilePath;
  }

  public String getLocalizedFileName() {
    return localizedFileName;
  }

  public LocalizableResource(String completeResourceString, FileSystem fs) throws ParseException, IOException  {
    this.completeResourceString = completeResourceString;
    this.parse(fs);
  }

  private void parse(FileSystem fs) throws ParseException, IOException {
    String filePath = completeResourceString;
    resourceType = LocalResourceType.FILE;
    if (completeResourceString.toLowerCase().endsWith(Constants.ARCHIVE_SUFFIX)) {
        resourceType = LocalResourceType.ARCHIVE;
        filePath = completeResourceString.substring(0, completeResourceString.length() - Constants.ARCHIVE_SUFFIX.length());
    }

    String[] tuple = filePath.split(Constants.RESOURCE_DIVIDER);
    if (tuple.length > 2) {
        throw new ParseException("Failed to parse file: " + completeResourceString);
    }
    sourceFilePath = new Path(tuple[0]);
    if (isLocalFile()) {
      FileSystem localFs = FileSystem.getLocal(fs.getConf());
      sourceFileStatus = localFs.getFileStatus(sourceFilePath);
    } else {
      sourceFileStatus = fs.getFileStatus(sourceFilePath);
    }
    localizedFileName = sourceFilePath.getName();

    if (tuple.length == 2) {
        localizedFileName = tuple[1];
    }
    if (sourceFileStatus.isDirectory()) {
        isDirectory = true;
    }
  }

  public LocalResource toLocalResource() {
    if (isDirectory) {
      throw new RuntimeException("Resource is directory and cannot be converted to LocalResource.");
    }
    return LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(
      URI.create(sourceFileStatus.getPath().toString())),
      resourceType, LocalResourceVisibility.PRIVATE,
      sourceFileStatus.getLen(), sourceFileStatus.getModificationTime());
  }

}
