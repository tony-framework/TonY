/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.azkaban;

// Standard TensorFlow submit arguments.
public enum TensorFlowJobArg {
  HDFS_CLASSPATH("hdfs_classpath"),
  SHELL_ENV("shell_env"),
  TASK_PARAMS("task_params"),
  PYTHON_BINARY_PATH("python_binary_path"),
  PYTHON_VENV("python_venv"),
  EXECUTES("executes"),
  SRC_DIR("src_dir");

  TensorFlowJobArg(String azPropName) {
    this.azPropName = azPropName;
    this.tfParamName = "-" + azPropName;
  }

  final String azPropName;
  final String tfParamName;
}
