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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.linkedin.tony.models.JobContainerRequest;
import com.linkedin.tony.util.Utils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

public final class HadoopCompatibleAdapter {
    private static final Log LOG = LogFactory.getLog(HadoopCompatibleAdapter.class);

    private static final AtomicLong ALLOCATE_ID_COUNTER = new AtomicLong(1);

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

    public static RegisterApplicationMasterResponse registerAppMaster(
            AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient,
            String appHostName,
            int appHostPort,
            String appTrackingUrl,
            String appLevelPlacementConstraintSpec) {
        try {
            Map<Set<String>, Object> placementConstraints = new HashMap<>();

            placementConstraints.put(
                    Collections.singleton(""),
                    parsePlacementConstraintSpec(appLevelPlacementConstraintSpec)
            );

            Method method = Arrays.stream(amRMClient.getClass().getMethods())
                    .filter(x -> x.getName().equals("registerApplicationMaster") && x.getParameterCount() == 4)
                    .findFirst().get();
            return (RegisterApplicationMasterResponse) method.invoke(amRMClient, appHostName, appHostPort,
                    appTrackingUrl, placementConstraints);
        } catch (Exception e) {
            throw new RuntimeException("Errors on registering app master.", e);
        }
    }

    public static void constructAndAddSchedulingRequest(AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient,
            JobContainerRequest containerRequest) {
        try {
            List<Object> reqs = new ArrayList<>();
            Object schedReq = constructSchedulingRequest(containerRequest);
            LOG.info("Request schedling containers ask: " + schedReq);
            for (int i = 0; i < containerRequest.getNumInstances(); i++) {
                reqs.add(schedReq);
            }
            Method addMethod = Arrays.stream(amRMClient.getClass().getMethods())
                    .filter(x -> x.getName().equals("addSchedulingRequests") && x.getParameterCount() == 1)
                    .findFirst().get();
            addMethod.invoke(amRMClient, reqs);
        } catch (Exception e) {
            throw new RuntimeException("Errors on adding scheduing request.", e);
        }
    }

    private static Object parsePlacementConstraintSpec(String spec) throws Exception {
        if (StringUtils.isEmpty(spec)) {
            return null;
        }

        Class<?> placementConstraintCls =
                Class.forName("org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser");
        Method parseMethod = placementConstraintCls.getMethod("parseExpression", String.class);

        Object parsedObj = parseMethod.invoke(placementConstraintCls, spec);
        Class<?> abstractConstraintCls =
                Class.forName("org.apache.hadoop.yarn.api.resource.PlacementConstraint$AbstractConstraint");

        Object placementConstraintObj = abstractConstraintCls.getMethod("build").invoke(parsedObj);
        return placementConstraintObj;
    }

    private static Object constructSchedulingRequest(JobContainerRequest containerRequest) {
        try {
            Priority priority = Priority.newInstance(containerRequest.getPriority());
            Resource capability = Resource.newInstance((int) containerRequest.getMemory(), containerRequest.getVCores());
            if (containerRequest.getGPU() > 0) {
                Utils.setCapabilityGPU(capability, containerRequest.getGPU());
            }
            Set<String> allocationTags = CollectionUtils.isEmpty(containerRequest.getAllocationTags())
                    ? Collections.singleton("") : new HashSet<>(containerRequest.getAllocationTags());

            Object placementConstraintObj = parsePlacementConstraintSpec(containerRequest.getPlacementSpec());
            if (StringUtils.isNotEmpty(containerRequest.getPlacementSpec())) {
                Class<?> placementConstraintCls =
                        Class.forName("org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser");
                Method parseMethod = placementConstraintCls.getMethod("parseExpression", String.class);

                Object parsedObj = parseMethod.invoke(placementConstraintCls, containerRequest.getPlacementSpec());
                Class<?> abstractConstraintCls =
                        Class.forName("org.apache.hadoop.yarn.api.resource.PlacementConstraint$AbstractConstraint");

                placementConstraintObj = abstractConstraintCls.getMethod("build").invoke(parsedObj);
            }

            Class<?> resourceSizingCls = Class.forName("org.apache.hadoop.yarn.api.records.ResourceSizing");
            Method resourceSizingMethod = Arrays.stream(resourceSizingCls.getMethods())
                    .filter(x -> x.getName().equals("newInstance") && x.getParameterCount() == 1).findFirst().get();
            Object resourceSizingObj = resourceSizingMethod.invoke(null, capability);

            Class<?> schedulingReqCls = Class.forName("org.apache.hadoop.yarn.api.records.SchedulingRequest");
            Method newInstanceMethod = Arrays.stream(schedulingReqCls.getMethods())
                    .filter(x -> x.getName().equals("newInstance") && x.getParameterCount() == 6).findFirst().get();

            Object schedReq = newInstanceMethod.invoke(null, ALLOCATE_ID_COUNTER.incrementAndGet(), priority,
                    ExecutionTypeRequest.newInstance(), allocationTags,
                    resourceSizingObj, placementConstraintObj);

            return schedReq;
        } catch (Exception e) {
            throw new RuntimeException("Errors on constructing scheduling requests of Yarn.", e);
        }
    }
}
