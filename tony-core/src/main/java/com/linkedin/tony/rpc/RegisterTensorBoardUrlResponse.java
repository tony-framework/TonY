/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;


public interface RegisterTensorBoardUrlResponse {
  String getSpec();
  void setSpec(String spec);
}
