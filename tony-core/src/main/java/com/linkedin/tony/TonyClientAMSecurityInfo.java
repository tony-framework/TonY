/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;


import com.linkedin.tony.rpc.MetricsRpc;
import com.linkedin.tony.rpc.TensorFlowClusterPB;
import java.lang.annotation.Annotation;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSelector;


@Public
@Stable
public class TonyClientAMSecurityInfo extends SecurityInfo {

  @Override
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    return null;
  }

  @Override
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    if (!protocol.equals(TensorFlowClusterPB.class) && !protocol.equals(MetricsRpc.class)) {
      return null;
    }

    return new TokenInfo() {
      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public Class<? extends TokenSelector<? extends TokenIdentifier>> value() {
        return ClientToAMTokenSelector.class;
      }
    };
  }
}

