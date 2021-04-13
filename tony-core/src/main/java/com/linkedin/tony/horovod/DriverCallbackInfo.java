/**
 * Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.horovod;

import java.util.List;

public class DriverCallbackInfo {
    private String port;
    private String host;
    private List<SlotInfo> slotInfos;

    public DriverCallbackInfo() {
        // ignore
    }

    public DriverCallbackInfo(String port, String host, List<SlotInfo> slotInfos) {
        this.port = port;
        this.host = host;
        this.slotInfos = slotInfos;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public List<SlotInfo> getSlotInfos() {
        return slotInfos;
    }

    public void setSlotInfos(List<SlotInfo> slotInfos) {
        this.slotInfos = slotInfos;
    }
}
