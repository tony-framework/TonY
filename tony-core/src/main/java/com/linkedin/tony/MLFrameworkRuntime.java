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

import java.io.IOException;

import com.linkedin.tony.runtime.HorovodRuntime;
import com.linkedin.tony.runtime.MXNetRuntime;
import com.linkedin.tony.runtime.PyTorchRuntime;
import com.linkedin.tony.runtime.TFRuntime;
import com.linkedin.tony.tensorflow.TonySession;

public interface MLFrameworkRuntime {
    static MLFrameworkRuntime get(TonyConfigurationKeys.MLFramework mlFramework) {
        switch (mlFramework) {
            case TENSORFLOW:
                return new TFRuntime();
            case PYTORCH:
                return new PyTorchRuntime();
            case HOROVOD:
                return new HorovodRuntime();
            case MXNET:
                return new MXNetRuntime();
            default:
                throw new RuntimeException("Unsupported executor framework: " + mlFramework);
        }
    }

    /** For AM, getting cluster spec and return to task exectuor **/
    String constructClusterSpec(final TonySession session, String taskId) throws IOException;

    /** For AM, when app finished, it need to call it to release resource **/
    void destory();

    /** For TaskExecutor, set the runtime environment before exec python process **/
    void buildTaskEnv(final TaskExecutor executor) throws Exception;
}
