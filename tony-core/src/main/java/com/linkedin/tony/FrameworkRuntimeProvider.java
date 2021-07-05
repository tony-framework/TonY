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
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FrameworkRuntimeProvider {
    private static final Log LOG = LogFactory.getLog(FrameworkRuntimeProvider.class);

    private static Map<String, AbstractFrameworkRuntime> runtimeServices = new HashMap<>();
    static {
        ServiceLoader<AbstractFrameworkRuntime> serviceLoader = ServiceLoader.load(AbstractFrameworkRuntime.class);
        Iterator<AbstractFrameworkRuntime> it = serviceLoader.iterator();
        while (it.hasNext()) {
            AbstractFrameworkRuntime runtime = null;
            try {
                runtime = it.next();
                runtimeServices.put(runtime.getFrameworkType().toUpperCase(), runtime);
            } catch (Exception e) {
                if (runtime != null) {
                    LOG.error("Errors on loading framework runtime class: " + runtime.getClass(), e);
                } else {
                    LOG.error("Errors on loading framework runtime class.", e);
                }
            }
        }
    }

    private FrameworkRuntimeProvider() {
        // ignore
    }

    public static Framework.ApplicationMasterAdapter getAMAdapter(String frameworkType) {
        if (!runtimeServices.containsKey(frameworkType.toUpperCase())) {
            throw new RuntimeException("Unsupported executor framework: " + frameworkType);
        }

        return runtimeServices.get(frameworkType.toUpperCase()).getAMAdapter();
    }

    public static Framework.TaskExecutorAdapter getTaskAdapter(String frameworkType,
            TaskExecutor executor) {
        if (!runtimeServices.containsKey(frameworkType.toUpperCase())) {
            throw new RuntimeException("Unsupported executor framework: " + frameworkType);
        }

        return runtimeServices.get(frameworkType.toUpperCase()).getTaskAdapter(executor);
    }
}
