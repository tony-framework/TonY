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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

public final class HadoopCompatibleAdapter {
    private static final Log LOG = LogFactory.getLog(HadoopCompatibleAdapter.class);

    private HadoopCompatibleAdapter() {

    }

    /**
     * Uses reflection to get memory size.
     * @param resource  the request container resource
     * @return the memory size
     */
    public static long getMemorySize(Resource resource) {
        Method method = null;
        try {
            method = resource.getClass().getMethod(Constants.GET_RESOURCE_MEMORY_SIZE);
        } catch (NoSuchMethodException nsne) {
            try {
                method = resource.getClass().getMethod(Constants.GET_RESOURCE_MEMORY_SIZE_DEPRECATED);
            } catch (NoSuchMethodException exception) {
                throw new RuntimeException(exception);
            }
        }

        try {
            Object memorySize = method.invoke(resource);
            if (memorySize instanceof Integer) {
                return ((Integer) memorySize).intValue();
            }
            return ((Long) memorySize).longValue();
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Failed to invoke '" + method.getName() + "' method to get memory size", e);
        }
    }

    /**
     * Gets the number of requested GPU in a Container. If GPU is not available on the cluster,
     * the function will return zero.
     */
    public static int getNumOfRequestedGPU(Container container) {
        int numGPU = 0;
        try {
            Class<?> resourceUtilsClass = Class.forName(Constants.HADOOP_RESOURCES_UTILS_CLASS);
            Constructor<?> constructor = resourceUtilsClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            ResourceUtils resourceUtils = (ResourceUtils) constructor.newInstance();
            Method method = resourceUtilsClass.getMethod(Constants.HADOOP_RESOURCES_UTILS_GET_RESOURCE_TYPE_INDEX_METHOD);
            Map<String, Integer> typeIndexMap = (Map<String, Integer>) method.invoke(resourceUtils);
            if (typeIndexMap.containsKey(Constants.GPU_URI)) {
                numGPU = (int) container.getResource().getResourceInformation(Constants.GPU_URI).getValue();
            }
        } catch (ClassNotFoundException | NoSuchMethodException
                | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return 0;
        }
        return numGPU;
    }

    /**
     * Parses Docker related configs and get required container launching environment.
     * Uses reflection to support older versions of Hadoop.
     */
    public static Map<String, String> getContainerEnvForDocker(Configuration tonyConf, String jobType) {
        Map<String, String> containerEnv = new HashMap<>();
        if (tonyConf.getBoolean(TonyConfigurationKeys.DOCKER_ENABLED, TonyConfigurationKeys.DEFAULT_DOCKER_ENABLED)) {
            String imagePath = tonyConf.get(TonyConfigurationKeys.getContainerDockerKey());
            String jobImagePath = tonyConf.get(TonyConfigurationKeys.getDockerImageKey(jobType));
            if (jobImagePath != null) {
                imagePath = jobImagePath;
            }
            if (imagePath == null) {
                LOG.error("Docker is enabled but " + TonyConfigurationKeys.getContainerDockerKey() + " is not set.");
                return containerEnv;
            } else {
                Class containerRuntimeClass = null;
                Class dockerRuntimeClass = null;
                try {
                    containerRuntimeClass = Class.forName(Constants.CONTAINER_RUNTIME_CONSTANTS_CLASS);
                    dockerRuntimeClass = Class.forName(Constants.DOCKER_LINUX_CONTAINER_RUNTIME_CLASS);
                } catch (ClassNotFoundException e) {
                    LOG.error("Docker runtime classes not found in this version ("
                            + org.apache.hadoop.util.VersionInfo.getVersion() + ") of Hadoop.", e);
                }
                if (dockerRuntimeClass != null) {
                    try {
                        String containerMounts = tonyConf.get(TonyConfigurationKeys.getContainerDockerMountKey());
                        if (StringUtils.isNotEmpty(containerMounts)) {
                            String envDockerContainerMounts =
                                    (String) dockerRuntimeClass.getField(Constants.ENV_DOCKER_CONTAINER_MOUNTS).get(null);
                            containerEnv.put(envDockerContainerMounts, containerMounts);
                        }

                        String envContainerType = (String) containerRuntimeClass.getField(Constants.ENV_CONTAINER_TYPE).get(null);
                        String envDockerImage = (String) dockerRuntimeClass.getField(Constants.ENV_DOCKER_CONTAINER_IMAGE).get(null);
                        containerEnv.put(envContainerType, "docker");
                        containerEnv.put(envDockerImage, imagePath);
                    } catch (NoSuchFieldException e) {
                        LOG.error("Field " + Constants.ENV_CONTAINER_TYPE + " or " + Constants.ENV_DOCKER_CONTAINER_IMAGE
                                + " or " + Constants.ENV_DOCKER_CONTAINER_MOUNTS + " not found in "
                                + containerRuntimeClass.getName() + " or " + dockerRuntimeClass.getName(), e);
                    } catch (IllegalAccessException e) {
                        LOG.error("Unable to access " + Constants.ENV_CONTAINER_TYPE + " or "
                                + Constants.ENV_DOCKER_CONTAINER_IMAGE + " or " + Constants.ENV_DOCKER_CONTAINER_MOUNTS
                                + " fields.", e);
                    }
                }
            }
        }
        return containerEnv;
    }

    public static boolean existGPUResource() {
        try {
            Class<?> resourceUtilsClass = Class.forName(Constants.HADOOP_RESOURCES_UTILS_CLASS);
            Constructor<?> constructor = resourceUtilsClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            ResourceUtils resourceUtils = (ResourceUtils) constructor.newInstance();
            Method method = resourceUtilsClass
                    .getMethod(Constants.HADOOP_RESOURCES_UTILS_GET_RESOURCE_TYPES_METHOD);
            Map<String, ResourceInformation> types = (Map<String, ResourceInformation>) method.invoke(resourceUtils);
            return types.containsKey(Constants.GPU_URI);
        } catch (ClassNotFoundException | NoSuchMethodException
                | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            LOG.error("With old Hadoop version, GPU is not supported.");
            return false;
        }
    }
}
