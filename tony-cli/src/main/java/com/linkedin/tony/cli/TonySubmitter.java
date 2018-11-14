/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.cli;

public abstract class TonySubmitter {
  public abstract int submit(String[] args) throws Exception;
}
