/*
 * Copyright 2021 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.tony;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestHadoopCompatibleAdapter {

    @Test
    public void testGetNumOfRequestedGPUWithGPUAvailable() {
        Resource resource = Resource.newInstance(256, 32);
        Container container = mock(Container.class);
        when(container.getResource()).thenReturn(resource);
        // Request 0 GPUs
        assertEquals(HadoopCompatibleAdapter.getNumOfRequestedGPU(container), 0);

        // Request 2 GPUs.
        resource.setResourceInformation(
                ResourceInformation.GPU_URI, ResourceInformation.newInstance(ResourceInformation.GPU_URI, "", 2));
        assertEquals(HadoopCompatibleAdapter.getNumOfRequestedGPU(container), 2);
    }

    @Test
    public void testGetNumOfRequestedGPUWithGPUUnavailable() {
        Container container = mock(Container.class);
        Resource resource = Resource.newInstance(256, 32);
        resource.setResourceInformation(
                ResourceInformation.GPU_URI, ResourceInformation.newInstance(ResourceInformation.GPU_URI, "", 2));
        when(container.getResource()).thenReturn(resource);  // Request 2 GPUs in the container.

        Map<String, ResourceInformation> defaultResourceTypes = ResourceUtils.getResourceTypes();
        try {
            // Mock that GPU is not available on cluster.
            ResourceUtils.initializeResourcesFromResourceInformationMap(ImmutableMap.of(
                    ResourceInformation.MEMORY_URI, ResourceInformation.MEMORY_MB,
                    ResourceInformation.VCORES_URI, ResourceInformation.VCORES
            ));
            assertEquals(HadoopCompatibleAdapter.getNumOfRequestedGPU(container), 0);
        } finally {
            // Reset to default resource types.
            ResourceUtils.initializeResourcesFromResourceInformationMap(defaultResourceTypes);
        }
    }

    @Test
    public void testGetContainerEnvForDocker() {
        Configuration conf = mock(Configuration.class);
        when(conf.getBoolean(TonyConfigurationKeys.DOCKER_ENABLED,
                TonyConfigurationKeys.DEFAULT_DOCKER_ENABLED))
                .thenReturn(true);
        assertEquals(HadoopCompatibleAdapter.getContainerEnvForDocker(conf, "tony.worker.gpus"),
                new HashMap<>());

        when(conf.get(TonyConfigurationKeys
                .getDockerImageKey("tony.worker.gpus"))).thenReturn("foo");
        assertEquals(HadoopCompatibleAdapter.getContainerEnvForDocker(conf, "tony.worker.gpus"),
                new HashMap<String, String>() {{
                    put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
                    put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", "foo");
                }});
    }
}
