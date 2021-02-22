/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.security;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;

/**
 * This class provides user facing APIs for transferring secrets from the job
 * client to the tasks. The secrets can be stored just before submission of jobs
 * and read during the task execution.
 */
public class TokenCache {

  private static final Log LOG = LogFactory.getLog(TokenCache.class);

  private TokenCache() {
    // Prevent instantiation
  }

  /**
   * Convenience method to obtain delegation tokens from namenodes corresponding
   * to the paths passed.
   * 
   * @param credentials
   *          cache in which to add new delegation tokens
   * @param paths
   *          array of paths
   * @param conf
   *          configuration
   * @throws IOException
   */
  public static void obtainTokensForNamenodes(Credentials credentials, Path[] paths, Configuration conf, String renewer) throws IOException {
    Set<FileSystem> fsSet = new HashSet<>();
    for (Path path : paths) {
      fsSet.add(path.getFileSystem(conf));
    }
    for (FileSystem fs : fsSet) {
      try {
        obtainTokensForNamenodesInternal(fs, credentials, renewer);
      } catch (Exception e) {
        LOG.error("Errors on getting delegation token for " + fs.getUri(), e);
      }
    }
  }

  /**
   * Get delegation token for a specific FS.
   * 
   * @param fs
   *          file system
   * @param credentials
   *          cache in which to add new delegation tokens
   * @param renewer
   *          the user allowed to renew the delegation tokens
   * @throws IOException
   */
  private static void obtainTokensForNamenodesInternal(FileSystem fs, Credentials credentials, String renewer) throws IOException {
    final Token<?>[] tokens = fs.addDelegationTokens(renewer, credentials);
    if (tokens != null) {
      for (Token<?> token : tokens) {
        LOG.info("Got delegation token for " + fs.getUri() + "; " + token);
      }
    }
  }

}
