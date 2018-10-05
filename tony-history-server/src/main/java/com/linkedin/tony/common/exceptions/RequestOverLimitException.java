/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.common.exceptions;

public class RequestOverLimitException extends THSExecException {

  private static final long serialVersionUID = 1L;

  public RequestOverLimitException(String message) {
    super(message);
  }
}
