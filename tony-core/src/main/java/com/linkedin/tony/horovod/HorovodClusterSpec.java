/**
 * Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.horovod;

import java.util.List;

public class HorovodClusterSpec {
    private List<SlotInfo> slotInfos;
    private int port;
    private String amHost;
    private List<Integer> sameHostTaskIndexList;

    public HorovodClusterSpec() {
        // ignore
    }

    public HorovodClusterSpec(List<SlotInfo> slotInfos, int port, String amHost, List<Integer> sameHostTaskIndexList) {
        this.slotInfos = slotInfos;
        this.port = port;
        this.amHost = amHost;
        this.sameHostTaskIndexList = sameHostTaskIndexList;
    }

    public List<SlotInfo> getSlotInfos() {
        return slotInfos;
    }

    public void setSlotInfos(List<SlotInfo> slotInfos) {
        this.slotInfos = slotInfos;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getAmHost() {
        return amHost;
    }

    public void setAmHost(String amHost) {
        this.amHost = amHost;
    }

    public List<Integer> getSameHostTaskIndexList() {
        return sameHostTaskIndexList;
    }

    public void setSameHostTaskIndexList(List<Integer> sameHostTaskIndexList) {
        this.sameHostTaskIndexList = sameHostTaskIndexList;
    }
}
