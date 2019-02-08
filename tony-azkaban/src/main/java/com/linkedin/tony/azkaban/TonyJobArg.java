/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.azkaban;

// Standard TonY job arguments.
public enum TonyJobArg {
  HDFS_CLASSPATH("hdfs_classpath"),
  SHELL_ENV("shell_env"),
  TASK_PARAMS("task_params"),
  PYTHON_BINARY_PATH("python_binary_path"),
  PYTHON_VENV("python_venv"),
  EXECUTES("executes"),
  SRC_DIR("src_dir");

  TonyJobArg(String azPropName) {
    this.azPropName = azPropName;
    this.tonyParamName = "-" + azPropName;
  }

  final String azPropName;
  final String tonyParamName;
}
