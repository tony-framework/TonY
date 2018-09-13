/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.tensorflow.TensorFlowContainerRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTensorFlowContainerRequest {
  @Test
  public void testMatchesResourceRequest() {
    // Container request for 1024 MB, 1 core, 1 GPU
    TensorFlowContainerRequest containerRequest = new TensorFlowContainerRequest("worker", 1, 1024, 1, 0);

    // Resource request for 1024 MB, 1 core, should not match (does not include GPUs)
    Resource resource = Resource.newInstance(1024, 1);
    Assert.assertFalse(containerRequest.matchesResourceRequest(resource));

    // Now add 1 GPU, should match
    resource.setResourceValue(ResourceInformation.GPU_URI, 1);
    Assert.assertTrue(containerRequest.matchesResourceRequest(resource));
  }
}
