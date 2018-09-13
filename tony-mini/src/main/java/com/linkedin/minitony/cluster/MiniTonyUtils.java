/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.minitony.cluster;

import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.conf.Configuration;


public class MiniTonyUtils {

  /**
   * Write a Hadoop configuration to file.
   * @param conf the configuration object.
   * @param filePath the filePath we are writing the configuration to.
   * @throws IOException IO exception during writing files.
   */
  public static void saveConfigToFile(Configuration conf, String filePath) throws IOException {
    PrintWriter yarnWriter = new PrintWriter(filePath, "UTF-8");
    conf.writeXml(yarnWriter);
  }

  private MiniTonyUtils() { }

}
