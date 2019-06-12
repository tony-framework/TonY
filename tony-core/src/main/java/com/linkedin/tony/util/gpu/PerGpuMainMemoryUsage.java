/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util.gpu;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


@InterfaceAudience.Private
@InterfaceStability.Unstable
@XmlRootElement(name = "bar1_memory_usage")
public class PerGpuMainMemoryUsage {
  long usedMemoryMiB = -1L;
  long availMemoryMiB = -1L;

  @XmlJavaTypeAdapter(PerGpuDeviceInformation.StrToMemAdapter.class)
  @XmlElement(name = "used")
  public Long getUsedMemoryMiB() {
    return usedMemoryMiB;
  }

  public void setUsedMemoryMiB(Long usedMemoryMiB) {
    this.usedMemoryMiB = usedMemoryMiB;
  }

  @XmlJavaTypeAdapter(PerGpuDeviceInformation.StrToMemAdapter.class)
  @XmlElement(name = "free")
  public Long getAvailMemoryMiB() {
    return availMemoryMiB;
  }

  public void setAvailMemoryMiB(Long availMemoryMiB) {
    this.availMemoryMiB = availMemoryMiB;
  }

  public long getTotalMemoryMiB() {
    return usedMemoryMiB + availMemoryMiB;
  }
}
