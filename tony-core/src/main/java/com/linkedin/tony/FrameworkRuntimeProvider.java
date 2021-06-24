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

import com.linkedin.tony.runtime.HorovodRuntime;
import com.linkedin.tony.runtime.MXNetRuntime;
import com.linkedin.tony.runtime.PyTorchRuntime;
import com.linkedin.tony.runtime.StandaloneRuntime;
import com.linkedin.tony.runtime.TFRuntime;

public class FrameworkRuntimeProvider {

    private FrameworkRuntimeProvider() {
        // ignore
    }

    public static Framework.ApplicationMasterAdapter getAMAdapter(TonyConfigurationKeys.FrameworkType frameworkType) {
        switch (frameworkType) {
            case TENSORFLOW:
                return new TFRuntime().getAMAdapter();
            case PYTORCH:
                return new PyTorchRuntime().getAMAdapter();
            case MXNET:
                return new MXNetRuntime().getAMAdapter();
            case HOROVOD:
                return new HorovodRuntime().getAMAdapter();
            case STANDALONE:
                return new StandaloneRuntime().getAMAdapter();
            default:
                throw new RuntimeException("Unsupported executor framework: " + frameworkType);
        }
    }

    public static Framework.TaskExecutorAdapter getTaskAdapter(TonyConfigurationKeys.FrameworkType frameworkType,
            TaskExecutor executor) {
        switch (frameworkType) {
            case TENSORFLOW:
                return new TFRuntime().getTaskAdapter(executor);
            case PYTORCH:
                return new PyTorchRuntime().getTaskAdapter(executor);
            case MXNET:
                return new MXNetRuntime().getTaskAdapter(executor);
            case HOROVOD:
                return new HorovodRuntime().getTaskAdapter(executor);
            case STANDALONE:
                return new StandaloneRuntime().getTaskAdapter(executor);
            default:
                throw new RuntimeException("Unsupported executor framework: " + frameworkType);
        }
    }
}
