/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.tony.util.gpu;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * Capture single GPU device information such as memory size, temperature,
 * utilization.
 *
 * Ported from Hadoop 2.9.0 with Temperature attribute removed
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@XmlRootElement(name = "gpu")
public class PerGpuDeviceInformation {

  private String productName = "N/A";
  private String uuid = "N/A";
  private int minorNumber = -1;

  private PerGpuUtilizations gpuUtilizations;
  private PerGpuFBMemoryUsage gpuFBMemoryUsage;
  private PerGpuMainMemoryUsage gpuMainMemoryUsage;

  /**
   * Convert formats like "34 C", "75.6 %" to float.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static class StrToFloatBeforeSpaceAdapter extends XmlAdapter<String, Float> {
    @Override
    public String marshal(Float v) throws Exception {
      if (v == null) {
        return "";
      }
      return String.valueOf(v);
    }

    @Override
    public Float unmarshal(String v) throws Exception {
      if (v == null) {
        return -1f;
      }

      return Float.valueOf(v.split(" ")[0]);
    }
  }

  /**
   * Convert formats like "725 MiB" to long.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static class StrToMemAdapter extends XmlAdapter<String, Long> {
    @Override
    public String marshal(Long v) throws Exception {
      if (v == null) {
        return "";
      }
      return String.valueOf(v) + " MiB";
    }

    @Override
    public Long unmarshal(String v) throws Exception {
      if (v == null) {
        return -1L;
      }
      return Long.valueOf(v.split(" ")[0]);
    }
  }

  @XmlElement(name = "uuid")
  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  @XmlElement(name = "product_name")
  public String getProductName() {
    return productName;
  }

  public void setProductName(String productName) {
    this.productName = productName;
  }

  @XmlElement(name = "minor_number")
  public int getMinorNumber() {
    return minorNumber;
  }

  public void setMinorNumber(int minorNumber) {
    this.minorNumber = minorNumber;
  }

  @XmlElement(name = "utilization")
  public PerGpuUtilizations getGpuUtilizations() {
    return gpuUtilizations;
  }

  public void setGpuUtilizations(PerGpuUtilizations utilizations) {
    this.gpuUtilizations = utilizations;
  }

  @XmlElement(name = "fb_memory_usage")
  public PerGpuFBMemoryUsage getGpuFBMemoryUsage() {
    return gpuFBMemoryUsage;
  }

  @XmlElement(name = "bar1_memory_usage")
  public PerGpuMainMemoryUsage getGpuMainMemoryUsage() {
    return gpuMainMemoryUsage;
  }

  public void setGpuFBMemoryUsage(PerGpuFBMemoryUsage gpuFBMemoryUsage) {
    this.gpuFBMemoryUsage = gpuFBMemoryUsage;
  }

  public void setGpuMainMemoryUsage(PerGpuMainMemoryUsage gpuMainMemoryUsage) {
    this.gpuMainMemoryUsage = gpuMainMemoryUsage;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ProductName=").append(productName).append(", MinorNumber=")
        .append(minorNumber);

    if (getGpuFBMemoryUsage() != null) {
      sb.append(", FBMemory=").append(
          getGpuFBMemoryUsage().getTotalMemoryMiB()).append("MiB");
    }

    if (getGpuMainMemoryUsage() != null) {
      sb.append(", Bar1Memory=").append(
          getGpuMainMemoryUsage().getTotalMemoryMiB()).append("MiB");
    }

    if (getGpuUtilizations() != null) {
      sb.append(", Utilization=").append(
          getGpuUtilizations().getOverallGpuUtilization()).append("%");
    }
    return sb.toString();
  }
}
