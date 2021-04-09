/**
 * Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.horovod;

import com.google.gson.annotations.SerializedName;

public class SlotInfo {
    private String hostname;
    private int rank;
    @SerializedName("local_rank")
    private int localRank;
    @SerializedName("cross_rank")
    private int crossRank;
    private int size;
    @SerializedName("local_size")
    private int localSize;
    @SerializedName("cross_size")
    private int crossSize;

    public SlotInfo() {
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public int getLocalRank() {
        return localRank;
    }

    public void setLocalRank(int localRank) {
        this.localRank = localRank;
    }

    public int getCrossRank() {
        return crossRank;
    }

    public void setCrossRank(int crossRank) {
        this.crossRank = crossRank;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getLocalSize() {
        return localSize;
    }

    public void setLocalSize(int localSize) {
        this.localSize = localSize;
    }

    public int getCrossSize() {
        return crossSize;
    }

    public void setCrossSize(int crossSize) {
        this.crossSize = crossSize;
    }
}
