/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;


public interface RegisterWorkerSpecRequest {
  String getWorker();
  String getSpec();
  void setWorker(String worker);
  void setSpec(String spec);
}