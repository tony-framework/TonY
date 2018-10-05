/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.common.exceptions;

public class ContainerRuntimeException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public ContainerRuntimeException() {
  }

  public ContainerRuntimeException(String message) {
    super(message);
  }

  public ContainerRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public ContainerRuntimeException(Throwable cause) {
    super(cause);
  }

  public ContainerRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
