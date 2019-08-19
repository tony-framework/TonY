/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.rpc.MetricsRpc;
import com.linkedin.tony.rpc.TensorFlowCluster;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;

/**
 * PolicyProvider for Client to AM protocol.
 **/
public class TonyPolicyProvider extends PolicyProvider {
    @Override
    public Service[] getServices() {
        return new Service[]{
            new Service("tony.cluster", TensorFlowCluster.class),
            new Service("tony.metrics", MetricsRpc.class)
        };
    }
}
