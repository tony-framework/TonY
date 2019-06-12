/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util.gpu;

/**
 * Ported from Hadoop 2.9.0, renamed from YarnException
 */
public class GpuInfoException extends Exception {
  public GpuInfoException() {
    super();
  }

  public GpuInfoException(String message) {
    super(message);
  }

  public GpuInfoException(Throwable cause) {
    super(cause);
  }

}
